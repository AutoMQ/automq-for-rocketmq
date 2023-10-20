/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.automq.stream.s3.wal;

import com.automq.stream.s3.wal.util.WALChannel;
import com.automq.stream.s3.wal.util.WALUtil;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.automq.stream.s3.wal.BlockWALService.RECORD_HEADER_MAGIC_CODE;
import static com.automq.stream.s3.wal.BlockWALService.RECORD_HEADER_SIZE;
import static com.automq.stream.s3.wal.BlockWALService.RECORD_HEADER_WITHOUT_CRC_SIZE;
import static com.automq.stream.s3.wal.BlockWALService.WAL_HEADER_TOTAL_CAPACITY;
import static com.automq.stream.s3.wal.WriteAheadLog.AppendResult;
import static com.automq.stream.s3.wal.WriteAheadLog.OverCapacityException;

/**
 * The sliding window contains all records that have not been flushed to the disk yet.
 * All records are written to the disk asynchronously by the AIO thread pool.
 * When the sliding window is full, the current thread will be blocked until the sliding window is expanded.
 * When the asynchronous write is completed, the start offset of the sliding window will be updated.
 */
public class SlidingWindowService {
    private static final Logger LOGGER = LoggerFactory.getLogger(SlidingWindowService.class.getSimpleName());
    private final int ioThreadNums;
    private final long upperLimit;
    private final long scaleUnit;
    private final WALChannel walChannel;
    private final WALHeaderFlusher walHeaderFlusher;
    private final WindowCoreData windowCoreData = new WindowCoreData();
    private ExecutorService executorService;

    private final Lock taskLock = new ReentrantLock();

    /**
     * Pending write task queue.
     */
    private final Queue<WriteBlockTask> writeTasks = new LinkedList<>();

    /**
     * The current block, records are added to this block.
     */
    private WriteBlockTask currentWriteTask;

    public SlidingWindowService(WALChannel walChannel, int ioThreadNums, long upperLimit, long scaleUnit, WALHeaderFlusher flusher) {
        this.walChannel = walChannel;
        this.ioThreadNums = ioThreadNums;
        this.upperLimit = upperLimit;
        this.scaleUnit = scaleUnit;
        this.walHeaderFlusher = flusher;
    }

    public void resetWindowWhenRecoverOver(long startOffset, long nextWriteOffset, long maxLength) {
        windowCoreData.setWindowStartOffset(startOffset);
        windowCoreData.setWindowNextWriteOffset(nextWriteOffset);
        windowCoreData.setWindowMaxLength(maxLength);
    }

    public WindowCoreData getWindowCoreData() {
        return windowCoreData;
    }

    public void start() throws IOException {
        this.executorService = Threads.newFixedThreadPool(ioThreadNums,
                ThreadUtils.createThreadFactory("block-wal-io-thread-%d", false), LOGGER);
    }

    public boolean shutdown(long timeout, TimeUnit unit) {
        this.executorService.shutdown();
        try {
            return this.executorService.awaitTermination(timeout, unit);
        } catch (InterruptedException e) {
            this.executorService.shutdownNow();
            return false;
        }
    }

    @Deprecated
    public long allocateWriteOffset(final int recordBodyLength, final long trimOffset, final long recordSectionCapacity) throws OverCapacityException {
        int totalWriteSize = RECORD_HEADER_SIZE + recordBodyLength;

        long lastWriteOffset;
        long newWriteOffset;
        long expectedWriteOffset;
        do {
            lastWriteOffset = windowCoreData.getWindowNextWriteOffset();
            expectedWriteOffset = lastWriteOffset;

            // If the end of the physical device is insufficient for this write, jump to the start of the physical device
            if ((recordSectionCapacity - expectedWriteOffset % recordSectionCapacity) < totalWriteSize) {
                expectedWriteOffset = expectedWriteOffset + recordSectionCapacity - expectedWriteOffset % recordSectionCapacity;
            }

            if (expectedWriteOffset + totalWriteSize - trimOffset > recordSectionCapacity) {
                // Not enough space for this write
                LOGGER.error("failed to allocate write offset as the ring buffer is full: expectedWriteOffset: {}, totalWriteSize: {}, trimOffset: {}, recordSectionCapacity: {}",
                        expectedWriteOffset, totalWriteSize, trimOffset, recordSectionCapacity);
                throw new OverCapacityException(String.format("failed to allocate write offset: ring buffer is full: expectedWriteOffset: %d, totalWriteSize: %d, trimOffset: %d, recordSectionCapacity: %d",
                        expectedWriteOffset, totalWriteSize, trimOffset, recordSectionCapacity));
            }

            newWriteOffset = WALUtil.alignLargeByBlockSize(expectedWriteOffset + totalWriteSize);
        } while (!windowCoreData.compareAndSetWindowNextWriteOffset(lastWriteOffset, newWriteOffset));

        return expectedWriteOffset;
    }

    public void submitWriteRecordTask(WriteBlockTask ioTask) {
        executorService.submit(new WriteRecordTaskProcessor(ioTask));
    }

    public Lock getTaskLock() {
        return taskLock;
    }

    /**
     * Create a new block. It
     * - puts the previous block to the write queue
     * - creates a new block, sets it as the current block and returns it
     * Note: this method is NOT thread safe, and it should be called with {@link #taskLock} locked.
     */
    public WriteBlockTask newBlockLocked(WriteBlockTask previousBlock, long minSize, long trimOffset, long recordSectionCapacity) throws OverCapacityException {
        long startOffset = previousBlock.startOffset() + WALUtil.alignLargeByBlockSize(previousBlock.data().limit());

        // If the end of the physical device is insufficient for this block, jump to the start of the physical device
        if ((recordSectionCapacity - startOffset % recordSectionCapacity) < minSize) {
            startOffset = startOffset + recordSectionCapacity - startOffset % recordSectionCapacity;
        }

        // Not enough space for this block
        if (startOffset + minSize - trimOffset > recordSectionCapacity) {
            LOGGER.error("failed to allocate write offset as the ring buffer is full: startOffset: {}, minSize: {}, trimOffset: {}, recordSectionCapacity: {}",
                    startOffset, minSize, trimOffset, recordSectionCapacity);
            throw new OverCapacityException(String.format("failed to allocate write offset: ring buffer is full: startOffset: %d, minSize: %d, trimOffset: %d, recordSectionCapacity: %d",
                    startOffset, minSize, trimOffset, recordSectionCapacity));
        }

        // The size of the block should not be larger than the end of the physical device
        long maxSize = Math.min(recordSectionCapacity - startOffset + trimOffset, upperLimit);

        WriteBlockTask newBlock = new WriteBlockTaskImpl(startOffset, maxSize);
        writeTasks.add(previousBlock);
        currentWriteTask = newBlock;
        return newBlock;
    }

    public WriteBlockTask getCurrentWriteTask() {
        return currentWriteTask;
    }

    private void writeRecord(WriteBlockTask ioTask) throws IOException {
        // TODO: make this beautiful
        long position = WALUtil.recordOffsetToPosition(ioTask.startOffset(), walChannel.capacity() - WAL_HEADER_TOTAL_CAPACITY, WAL_HEADER_TOTAL_CAPACITY);

        walChannel.write(ioTask.data(), position);
    }

    private boolean makeWriteOffsetMatchWindow(final WriteBlockTask writeBlockTask) throws IOException {
        long newWindowEndOffset = writeBlockTask.startOffset() + writeBlockTask.data().limit();
        // align to block size
        newWindowEndOffset = WALUtil.alignLargeByBlockSize(newWindowEndOffset);
        long windowStartOffset = windowCoreData.getWindowStartOffset();
        long windowMaxLength = windowCoreData.getWindowMaxLength();
        if (newWindowEndOffset > windowStartOffset + windowMaxLength) {
            long newWindowMaxLength = newWindowEndOffset - windowStartOffset + scaleUnit;
            if (newWindowMaxLength > upperLimit) {
                // exceed upper limit
                if (newWindowEndOffset - windowStartOffset >= upperLimit) {
                    // however, the new window length is still larger than upper limit, so we just set it to upper limit
                    newWindowMaxLength = upperLimit;
                } else {
                    // the new window length is bigger than upper limit, reject this write request
                    LOGGER.error("new windows size {} exceeds upper limit {}, reject this write request, window start offset: {}, new window end offset: {}",
                            newWindowMaxLength, upperLimit, windowStartOffset, newWindowEndOffset);
                    FutureUtil.completeExceptionally(writeBlockTask.futures(),
                            new OverCapacityException(String.format("new windows size exceeds upper limit %d", upperLimit)));
                    return false;
                }
            }
            windowCoreData.scaleOutWindow(walHeaderFlusher, newWindowMaxLength);
        }
        return true;
    }

    public static class RecordHeaderCoreData {
        private int magicCode0 = RECORD_HEADER_MAGIC_CODE;
        private int recordBodyLength1;
        private long recordBodyOffset2;
        private int recordBodyCRC3;
        private int recordHeaderCRC4;

        public static RecordHeaderCoreData unmarshal(ByteBuffer byteBuffer) {
            RecordHeaderCoreData recordHeaderCoreData = new RecordHeaderCoreData();
            recordHeaderCoreData.magicCode0 = byteBuffer.getInt();
            recordHeaderCoreData.recordBodyLength1 = byteBuffer.getInt();
            recordHeaderCoreData.recordBodyOffset2 = byteBuffer.getLong();
            recordHeaderCoreData.recordBodyCRC3 = byteBuffer.getInt();
            recordHeaderCoreData.recordHeaderCRC4 = byteBuffer.getInt();
            return recordHeaderCoreData;
        }

        public int getMagicCode() {
            return magicCode0;
        }

        public RecordHeaderCoreData setMagicCode(int magicCode) {
            this.magicCode0 = magicCode;
            return this;
        }

        public int getRecordBodyLength() {
            return recordBodyLength1;
        }

        public RecordHeaderCoreData setRecordBodyLength(int recordBodyLength) {
            this.recordBodyLength1 = recordBodyLength;
            return this;
        }

        public long getRecordBodyOffset() {
            return recordBodyOffset2;
        }

        public RecordHeaderCoreData setRecordBodyOffset(long recordBodyOffset) {
            this.recordBodyOffset2 = recordBodyOffset;
            return this;
        }

        public int getRecordBodyCRC() {
            return recordBodyCRC3;
        }

        public RecordHeaderCoreData setRecordBodyCRC(int recordBodyCRC) {
            this.recordBodyCRC3 = recordBodyCRC;
            return this;
        }

        public int getRecordHeaderCRC() {
            return recordHeaderCRC4;
        }

        public RecordHeaderCoreData setRecordHeaderCRC(int recordHeaderCRC) {
            this.recordHeaderCRC4 = recordHeaderCRC;
            return this;
        }

        @Override
        public String toString() {
            return "RecordHeaderCoreData{" +
                    "magicCode=" + magicCode0 +
                    ", recordBodyLength=" + recordBodyLength1 +
                    ", recordBodyOffset=" + recordBodyOffset2 +
                    ", recordBodyCRC=" + recordBodyCRC3 +
                    ", recordHeaderCRC=" + recordHeaderCRC4 +
                    '}';
        }

        private ByteBuffer marshalHeaderExceptCRC() {
            ByteBuffer byteBuffer = ByteBuffer.allocate(RECORD_HEADER_SIZE);
            byteBuffer.putInt(magicCode0);
            byteBuffer.putInt(recordBodyLength1);
            byteBuffer.putLong(recordBodyOffset2);
            byteBuffer.putInt(recordBodyCRC3);
            return byteBuffer;
        }

        public ByteBuffer marshal() {
            ByteBuffer byteBuffer = marshalHeaderExceptCRC();
            byteBuffer.putInt(WALUtil.crc32(byteBuffer, RECORD_HEADER_WITHOUT_CRC_SIZE));
            return byteBuffer.position(0);
        }
    }

    public static class WindowCoreData {
        private final Lock treeMapIOTaskRequestLock = new ReentrantLock();
        private final TreeMap<Long, WriteBlockTask> treeMapWriteRecordTask = new TreeMap<>();
        private final AtomicLong windowMaxLength = new AtomicLong(0);
        /**
         * Next write offset of sliding window, always aligned to the {@link WALUtil#BLOCK_SIZE}.
         */
        private final AtomicLong windowNextWriteOffset = new AtomicLong(0);
        /**
         * Start offset of sliding window, always aligned to the {@link WALUtil#BLOCK_SIZE}.
         * The data before this offset has already been written to the disk.
         */
        private final AtomicLong windowStartOffset = new AtomicLong(0);

        public long getWindowMaxLength() {
            return windowMaxLength.get();
        }

        public void setWindowMaxLength(long windowMaxLength) {
            this.windowMaxLength.set(windowMaxLength);
        }

        public long getWindowNextWriteOffset() {
            return windowNextWriteOffset.get();
        }

        public void setWindowNextWriteOffset(long windowNextWriteOffset) {
            this.windowNextWriteOffset.set(windowNextWriteOffset);
        }

        public boolean compareAndSetWindowNextWriteOffset(long expect, long update) {
            return this.windowNextWriteOffset.compareAndSet(expect, update);
        }

        public long getWindowStartOffset() {
            return windowStartOffset.get();
        }

        public void setWindowStartOffset(long windowStartOffset) {
            this.windowStartOffset.set(windowStartOffset);
        }

        public void putWriteRecordTask(WriteBlockTask writeBlockTask) {
            this.treeMapIOTaskRequestLock.lock();
            try {
                this.treeMapWriteRecordTask.put(writeBlockTask.startOffset(), writeBlockTask);
            } finally {
                this.treeMapIOTaskRequestLock.unlock();
            }
        }

        public void calculateStartOffset(long wroteOffset) {
            this.treeMapIOTaskRequestLock.lock();
            try {
                treeMapWriteRecordTask.remove(wroteOffset);

                if (treeMapWriteRecordTask.isEmpty()) {
                    setWindowStartOffset(getWindowNextWriteOffset());
                } else {
                    setWindowStartOffset(treeMapWriteRecordTask.firstKey());
                }
            } finally {
                this.treeMapIOTaskRequestLock.unlock();
            }
        }

        public void scaleOutWindow(WALHeaderFlusher flusher, long newWindowMaxLength) throws IOException {
            boolean scaleWindowHappened = false;
            treeMapIOTaskRequestLock.lock();
            try {
                if (newWindowMaxLength < windowMaxLength.get()) {
                    // Another thread has already scaled out the window.
                    return;
                }

                flusher.flush(newWindowMaxLength);
                setWindowMaxLength(newWindowMaxLength);
                scaleWindowHappened = true;
            } finally {
                treeMapIOTaskRequestLock.unlock();
                if (scaleWindowHappened) {
                    LOGGER.info("window scale out to {}", newWindowMaxLength);
                } else {
                    LOGGER.debug("window already scale out, ignore");
                }
            }
        }
    }

    class WriteRecordTaskProcessor implements Runnable {
        private final WriteBlockTask writeBlockTask;

        public WriteRecordTaskProcessor(WriteBlockTask writeBlockTask) {
            this.writeBlockTask = writeBlockTask;
        }

        @Override
        public void run() {
            try {
                if (makeWriteOffsetMatchWindow(writeBlockTask)) {

                    windowCoreData.putWriteRecordTask(writeBlockTask);

                    writeRecord(writeBlockTask);

                    // Update the start offset of the sliding window after finishing writing the record.
                    windowCoreData.calculateStartOffset(writeBlockTask.startOffset());

                    FutureUtil.complete(writeBlockTask.futures(), new AppendResult.CallbackResult() {
                        @Override
                        public long flushedOffset() {
                            return windowCoreData.getWindowStartOffset();
                        }

                        @Override
                        public String toString() {
                            return "CallbackResult{" + "flushedOffset=" + flushedOffset() + '}';
                        }
                    });
                }

            } catch (IOException e) {
                FutureUtil.completeExceptionally(writeBlockTask.futures(), e);
                LOGGER.error(String.format("failed to write record, offset: %s", writeBlockTask.startOffset()), e);
            }
        }
    }

    interface WALHeaderFlusher {
        void flush(long windowMaxLength) throws IOException;
    }
}
