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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

public class SnapshotService implements Lifecycle, Runnable {

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

    public CompletableFuture<Long> addSnapshotTask(SnapshotTask task) {
        CompletableFuture<Long> cf = new CompletableFuture<>();
        task.setSuccessCf(cf);
        snapshotTaskQueue.add(task);
        return cf;
    }

    @Override
    public void run() {
        while (!stopped) {
            try {
                SnapshotTask snapshotTask = snapshotTaskQueue.poll();
                if (snapshotTask == null) {
                    Thread.sleep(100);
                    continue;
                }
                takeSnapshot(snapshotTask)
                    .exceptionally(e -> {
                        snapshotTask.successCf.completeExceptionally(e);
                        return null;
                    }).join();
            } catch (Exception e) {
                // TODO: log it
            }
        }
    }

    public CompletableFuture<Void> takeSnapshot(SnapshotTask task) {
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
            byte[] tqPrefix = ByteBuffer.allocate(12)
                .putLong(topicId)
                .putInt(queueId)
                .array();
            List<CheckPoint> checkPointList = new ArrayList<>();
            try {
                kvService.iterate(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, tqPrefix, null, null, (key, value) -> {
                    CheckPoint checkPoint = SerializeUtil.decodeCheckPoint(ByteBuffer.wrap(value));
                    checkPointList.add(checkPoint);
                }, readOptions);
                // release snapshot
                kvService.releaseSnapshot(snapshot.getKvServiceSnapshotVersion());
            } catch (StoreException e) {
                throw new RuntimeException(e);
            }
            snapshot.setCheckPoints(checkPointList);
            byte[] snapshotData = SerializeUtil.encodeOperationSnapshot(snapshot);
            return snapshotData;
        });
        CompletableFuture<AppendResult> snapshotAppendCf = snapshotDataCf.thenCompose(snapshotData -> {
            // append snapshot to snapshot stream
            return streamStore.append(snapshotStreamId, new SingleRecord(ByteBuffer.wrap(snapshotData)));
        });
        return snapshotAppendCf.thenCombine(snapshotFuture, (appendResult, snapshot) -> {
            // trim operation stream
            streamStore.trim(operationStreamId, snapshot.getSnapshotEndOffset() + 1);
            task.successCf.complete(snapshot.getSnapshotEndOffset() + 1);
            // release snapshot
            return null;
        });
    }

    static class SnapshotTask {
        private final long topicId;
        private final int queueId;
        private final long operationStreamId;
        private final long snapshotStreamId;
        private final Supplier<CompletableFuture<OperationSnapshot>> snapshotSupplier;

        private CompletableFuture<Long> successCf;

        public SnapshotTask(long topicId, int queueId,
            long operationStreamId, long snapshotStreamId,
            Supplier<CompletableFuture<OperationSnapshot>> snapshotSupplier) {
            this.topicId = topicId;
            this.queueId = queueId;
            this.operationStreamId = operationStreamId;
            this.snapshotStreamId = snapshotStreamId;
            this.snapshotSupplier = snapshotSupplier;
        }

        public void setSuccessCf(CompletableFuture<Long> successCf) {
            this.successCf = successCf;
        }
    }
}
