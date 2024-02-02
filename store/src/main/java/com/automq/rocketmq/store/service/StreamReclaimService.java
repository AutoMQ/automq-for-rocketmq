/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.rocketmq.store.service;

import com.automq.rocketmq.common.util.Lifecycle;
import com.automq.rocketmq.store.api.StreamStore;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.ThreadUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamReclaimService implements Lifecycle, Runnable {
    // TODO: Unifying the same logic with other services
    public static final Logger LOGGER = LoggerFactory.getLogger(StreamReclaimService.class);
    private final StreamStore streamStore;
    private final BlockingQueue<StreamReclaimTask> taskQueue;
    private Thread streamReclaimer;
    private volatile boolean stopped = false;
    private CompletableFuture<Void> runningCf;
    private ExecutorService backgroundExecutor;

    public StreamReclaimService(StreamStore streamStore) {
        this.streamStore = streamStore;
        this.taskQueue = new LinkedBlockingQueue<>(1024);
        this.backgroundExecutor = Executors.newSingleThreadExecutor(
            ThreadUtils.createThreadFactory("stream-reclaim-background-executor", false)
        );
    }

    @Override
    public void start() throws Exception {
        this.stopped = false;
        this.runningCf = new CompletableFuture<>();
        if (this.backgroundExecutor == null || this.backgroundExecutor.isShutdown()) {
            this.backgroundExecutor = Executors.newSingleThreadExecutor(
                ThreadUtils.createThreadFactory("stream-reclaim-background-executor", false)
            );
        }
        this.streamReclaimer = new Thread(this, "stream-reclaimer");
        this.streamReclaimer.setDaemon(true);
        this.streamReclaimer.start();
    }

    @Override
    public void shutdown() throws Exception {
        this.stopped = true;
        // 1. wait for the current task to complete
        if (runningCf != null) {
            runningCf.join();
        }
        // 2. abort all waiting snapshot task
        List<StreamReclaimTask> tasks = new ArrayList<>();
        taskQueue.drainTo(tasks);
        tasks.forEach(StreamReclaimTask::abort);
        // 3. shutdown background executor
        if (this.backgroundExecutor != null) {
            this.backgroundExecutor.shutdown();
            this.backgroundExecutor = null;
        }
    }

    @Override
    public void run() {
        while (!stopped) {
            try {
                StreamReclaimTask task = taskQueue.poll(100, TimeUnit.MILLISECONDS);
                if (task == null) {
                    continue;
                }
                CompletableFuture<Void> takeCf = reclaimStream(task)
                    .exceptionally(e -> {
                        Throwable cause = FutureUtil.cause(e);
                        task.completeFailure(cause);
                        return null;
                    });
                takeCf.join();
            } catch (InterruptedException ignore) {
            } catch (Exception e) {
                Throwable cause = FutureUtil.cause(e);
                LOGGER.warn("Failed to reclaim stream", cause);
            }
        }
        runningCf.complete(null);
    }

    CompletableFuture<Void> reclaimStream(StreamReclaimTask task) {
        if (stopped) {
            task.abort();
            return CompletableFuture.completedFuture(null);
        }
        return task.streamIdCf.thenComposeAsync(streamId -> {
            long newStartOffset = task.newStartOffset;
            return streamStore.trim(streamId, newStartOffset)
                .thenAccept(nil -> task.completeSuccess(streamStore.startOffset(streamId)));
        }, backgroundExecutor);
    }

    public CompletableFuture<StreamReclaimResult> addReclaimTask(StreamReclaimTask task) {
        CompletableFuture<StreamReclaimResult> cf = new CompletableFuture<>();
        task.setCf(cf);
        taskQueue.add(task);
        return cf;
    }

    public int inflightTaskNum() {
        return taskQueue.size();
    }

    public static class StreamReclaimTask {
        private final CompletableFuture<Long> streamIdCf;
        private final long newStartOffset;
        private CompletableFuture<StreamReclaimResult> cf;

        public StreamReclaimTask(CompletableFuture<Long> streamIdCf, long newStartOffset) {
            this.streamIdCf = streamIdCf;
            this.newStartOffset = newStartOffset;
        }

        public void setCf(CompletableFuture<StreamReclaimResult> cf) {
            this.cf = cf;
        }

        public void completeFailure(Throwable cause) {
            LOGGER.warn("[StreamReclaimTask]: Reclaim stream resource: {} failed", this, cause);
            this.cf.completeExceptionally(cause);
        }

        public void completeSuccess(long offset) {
            this.cf.complete(StreamReclaimResult.ofSuccess(offset));
        }

        public void abort() {
            this.cf.complete(StreamReclaimResult.ofAbort());
        }

        @Override
        public String toString() {
            return "StreamReclaimTask{" +
                "streamIdCf=" + streamIdCf +
                ", newStartOffset=" + newStartOffset +
                ", cf=" + cf +
                '}';
        }
    }

    public static class StreamReclaimResult {
        private final boolean success;
        private final long startOffset;

        private StreamReclaimResult(boolean success, long startOffset) {
            this.success = success;
            this.startOffset = startOffset;
        }

        public static StreamReclaimResult ofSuccess(long startOffset) {
            return new StreamReclaimResult(true, startOffset);
        }

        public static StreamReclaimResult ofAbort() {
            return new StreamReclaimResult(false, -1);
        }

        public boolean success() {
            return success;
        }

        public long startOffset() {
            return startOffset;
        }
    }
}
