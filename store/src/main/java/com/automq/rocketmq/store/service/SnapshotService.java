/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.automq.rocketmq.store.service;

import com.automq.rocketmq.common.util.Lifecycle;
import com.automq.rocketmq.store.MessageStoreImpl;
import com.automq.rocketmq.store.api.StreamStore;
import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.model.generated.CheckPoint;
import com.automq.rocketmq.store.model.kv.KVReadOptions;
import com.automq.rocketmq.store.model.message.TopicQueueId;
import com.automq.rocketmq.store.model.operation.OperationSnapshot;
import com.automq.rocketmq.store.model.stream.SingleRecord;
import com.automq.rocketmq.store.service.api.KVService;
import com.automq.rocketmq.store.util.SerializeUtil;
import com.automq.stream.api.AppendResult;
import com.automq.stream.utils.FutureUtil;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnapshotService implements Lifecycle, Runnable {
    public static final Logger LOGGER = LoggerFactory.getLogger(SnapshotService.class);

    private final StreamStore streamStore;
    private final BlockingQueue<SnapshotTask> snapshotTaskQueue;
    private Thread snapshotTaker;
    private final KVService kvService;
    private volatile boolean stopped = false;
    private final ConcurrentMap<TopicQueueId, SnapshotStatus> snapshotStatusMap = new ConcurrentHashMap<>();
    private CompletableFuture<Void> runningCf;

    public SnapshotService(StreamStore streamStore, KVService kvService) {
        this.streamStore = streamStore;
        this.snapshotTaskQueue = new LinkedBlockingQueue<>(1024);
        this.kvService = kvService;
    }

    public static class SnapshotStatus {
        private final AtomicLong snapshotEndOffset = new AtomicLong(-1);
        private final AtomicLong operationStartOffset = new AtomicLong(-1);
        private final AtomicBoolean takingSnapshot = new AtomicBoolean(false);

        public AtomicLong snapshotEndOffset() {
            return snapshotEndOffset;
        }

        public AtomicLong operationStartOffset() {
            return operationStartOffset;
        }

        public AtomicBoolean takingSnapshot() {
            return takingSnapshot;
        }
    }

    public SnapshotStatus getSnapshotStatus(long topicId, int queueId) {
        return snapshotStatusMap.computeIfAbsent(TopicQueueId.of(topicId, queueId), k -> new SnapshotStatus());
    }

    @Override
    public void start() throws Exception {
        this.stopped = false;
        this.runningCf = new CompletableFuture<>();
        this.snapshotTaker = new Thread(this, "snapshot-taker");
        this.snapshotTaker.setDaemon(true);
        this.snapshotTaker.start();
    }

    @Override
    public void shutdown() throws Exception {
        this.stopped = true;
        // 1. wait for the current task to complete
        if (runningCf != null) {
            runningCf.join();
        }
        // 2. abort all waiting snapshot task
        List<SnapshotTask> snapshotTasks = new ArrayList<>();
        snapshotTaskQueue.drainTo(snapshotTasks);
        snapshotTasks.forEach(SnapshotTask::abort);
    }

    @Override
    public void run() {
        while (!stopped) {
            try {
                SnapshotTask task = snapshotTaskQueue.poll(100, TimeUnit.MILLISECONDS);
                if (task == null) {
                    continue;
                }
                CompletableFuture<Void> takeCf = takeSnapshot(task)
                    .exceptionally(e -> {
                        Throwable cause = FutureUtil.cause(e);
                        task.completeFailure(cause);
                        return null;
                    });
                takeCf.join();
            } catch (InterruptedException ignore) {
            } catch (Exception e) {
                Throwable cause = FutureUtil.cause(e);
                LOGGER.warn("Failed to take snapshot", cause);
            }
        }
        runningCf.complete(null);
    }

    CompletableFuture<Void> takeSnapshot(SnapshotTask task) {
        if (stopped) {
            task.abort();
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<OperationSnapshot> snapshotFuture = task.snapshotSupplier.get();
        long topicId = task.topicId;
        int queueId = task.queueId;
        long operationStreamId = task.operationStreamId;
        long snapshotStreamId = task.snapshotStreamId;
        CompletableFuture<byte[]> snapshotDataCf = snapshotFuture.thenApply(snapshot -> {
            long version = snapshot.getKvServiceSnapshotVersion();
            KVReadOptions readOptions = new KVReadOptions();
            readOptions.setSnapshotVersion(version);

            // get queue related checkpoints from kv service
            byte[] tqPrefix = SerializeUtil.buildCheckPointPrefix(topicId, queueId);
            List<CheckPoint> checkPointList = new ArrayList<>();
            try {
                kvService.iterate(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, tqPrefix, null, null, (key, value) -> {
                    CheckPoint checkPoint = SerializeUtil.decodeCheckPoint(ByteBuffer.wrap(value));
                    checkPointList.add(checkPoint);
                }, readOptions);
            } catch (StoreException e) {
                throw new CompletionException(e);
            } finally {
                // release snapshot
                try {
                    kvService.releaseSnapshot(snapshot.getKvServiceSnapshotVersion());
                } catch (StoreException e) {
                    LOGGER.error("Release snapshot:{} failed", snapshot, e);
                }
            }
            snapshot.setCheckPoints(checkPointList);
            return SerializeUtil.encodeOperationSnapshot(snapshot);
        });
        CompletableFuture<AppendResult> snapshotAppendCf = snapshotDataCf.thenCompose(snapshotData -> {
            // append snapshot to snapshot stream
            return streamStore.append(snapshotStreamId, new SingleRecord(ByteBuffer.wrap(snapshotData)));
        });
        return snapshotAppendCf.thenCombine(snapshotFuture, (appendResult, snapshot) -> snapshot).thenCompose(snapshot -> {
            // trim operation stream
            return streamStore.trim(operationStreamId, snapshot.getSnapshotEndOffset() + 1)
                .thenAccept(nil -> {
                    // complete snapshot task
                    task.completeSuccess(snapshot.getSnapshotEndOffset() + 1);
                });
        });
    }

    CompletableFuture<TakeSnapshotResult> addSnapshotTask(SnapshotTask task) {
        CompletableFuture<TakeSnapshotResult> cf = new CompletableFuture<>();
        task.setCf(cf);
        snapshotTaskQueue.add(task);
        return cf;
    }

    static class SnapshotTask {
        private final long topicId;
        private final int queueId;
        private final long operationStreamId;
        private final long snapshotStreamId;
        private final Supplier<CompletableFuture<OperationSnapshot>> snapshotSupplier;

        private CompletableFuture<TakeSnapshotResult> cf;

        public SnapshotTask(long topicId, int queueId, long operationStreamId, long snapshotStreamId,
            Supplier<CompletableFuture<OperationSnapshot>> snapshotSupplier) {
            this.topicId = topicId;
            this.queueId = queueId;
            this.operationStreamId = operationStreamId;
            this.snapshotStreamId = snapshotStreamId;
            this.snapshotSupplier = snapshotSupplier;
        }

        public void setCf(CompletableFuture<TakeSnapshotResult> cf) {
            this.cf = cf;
        }

        public void completeFailure(Throwable cause) {
            LOGGER.warn("[SnapshotTask]: Take snapshot: {} failed", this, cause);
            this.cf.completeExceptionally(cause);
        }

        public void completeSuccess(long offset) {
            this.cf.complete(TakeSnapshotResult.ofSuccess(offset));
        }

        public void abort() {
            this.cf.complete(TakeSnapshotResult.ofAbort());
        }

        @Override
        public String toString() {
            return "SnapshotTask{" +
                "topicId=" + topicId +
                ", queueId=" + queueId +
                ", operationStreamId=" + operationStreamId +
                ", snapshotStreamId=" + snapshotStreamId +
                '}';
        }
    }

    static class TakeSnapshotResult {
        private final boolean success;
        private final long newOpStartOffset;

        private TakeSnapshotResult(boolean success, long newOpStartOffset) {
            this.success = success;
            this.newOpStartOffset = newOpStartOffset;
        }

        public static TakeSnapshotResult ofSuccess(long newOpStartOffset) {
            return new TakeSnapshotResult(true, newOpStartOffset);
        }

        public static TakeSnapshotResult ofAbort() {
            return new TakeSnapshotResult(false, -1);
        }

        public boolean success() {
            return success;
        }

        public long newOpStartOffset() {
            return newOpStartOffset;
        }
    }
}
