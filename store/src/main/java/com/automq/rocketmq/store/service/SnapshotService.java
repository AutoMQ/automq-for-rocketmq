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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
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

    public SnapshotService(StreamStore streamStore, KVService kvService) {
        this.streamStore = streamStore;
        this.snapshotTaskQueue = new LinkedBlockingQueue<>(1024);
        this.kvService = kvService;
    }

    @Override
    public void start() throws Exception {
        this.stopped = false;
        this.snapshotTaker = new Thread(this, "snapshot-taker");
        this.snapshotTaker.setDaemon(true);
        this.snapshotTaker.start();
    }

    @Override
    public void shutdown() throws Exception {
        this.stopped = true;
        if (this.snapshotTaker != null) {
            this.snapshotTaker.interrupt();
        }
    }

    @Override
    public void run() {
        while (!stopped) {
            try {
                SnapshotTask snapshotTask = snapshotTaskQueue.poll(100, TimeUnit.MILLISECONDS);
                if (snapshotTask == null) {
                    continue;
                }
                takeSnapshot(snapshotTask)
                    .exceptionally(e -> {
                        Throwable cause = FutureUtil.cause(e);
                        snapshotTask.completeFailure(cause);
                        return null;
                    }).join();
            } catch (Exception e) {
                Throwable cause = FutureUtil.cause(e);
                LOGGER.warn("Failed to take snapshot", cause);
            }
        }
    }

    CompletableFuture<Void> takeSnapshot(SnapshotTask task) {
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
                    LOGGER.error("release snapshot failed", e);
                }
            }
            snapshot.setCheckPoints(checkPointList);
            return SerializeUtil.encodeOperationSnapshot(snapshot);
        });
        CompletableFuture<AppendResult> snapshotAppendCf = snapshotDataCf.thenCompose(snapshotData -> {
            // append snapshot to snapshot stream
            return streamStore.append(snapshotStreamId, new SingleRecord(ByteBuffer.wrap(snapshotData)));
        });
        return snapshotAppendCf.thenCombine(snapshotFuture, (appendResult, snapshot) -> {
            // trim operation stream
            streamStore.trim(operationStreamId, snapshot.getSnapshotEndOffset() + 1);
            task.completeSuccess(snapshot.getSnapshotEndOffset() + 1);
            // release snapshot
            return null;
        });
    }

    CompletableFuture<Long> addSnapshotTask(SnapshotTask task) {
        CompletableFuture<Long> cf = new CompletableFuture<>();
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

        private CompletableFuture<Long/*new start offset*/> cf;

        public SnapshotTask(long topicId, int queueId, long operationStreamId, long snapshotStreamId,
            Supplier<CompletableFuture<OperationSnapshot>> snapshotSupplier) {
            this.topicId = topicId;
            this.queueId = queueId;
            this.operationStreamId = operationStreamId;
            this.snapshotStreamId = snapshotStreamId;
            this.snapshotSupplier = snapshotSupplier;
        }

        public void setCf(CompletableFuture<Long> cf) {
            this.cf = cf;
        }

        public void completeFailure(Throwable cause) {
            LOGGER.warn("[SnapshotTask]: take snapshot failed", cause);
            this.cf.completeExceptionally(cause);
        }

        public void completeSuccess(long offset) {
            this.cf.complete(offset);
        }
    }
}
