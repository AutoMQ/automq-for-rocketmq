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

package com.automq.rocketmq.proxy.service;

import com.automq.rocketmq.common.ServiceThread;
import com.automq.rocketmq.proxy.model.ProxyContextExt;
import com.automq.rocketmq.store.model.message.Filter;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.instrumentation.annotations.SpanAttribute;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SuspendRequestService extends ServiceThread implements StartAndShutdown {
    protected static final Logger LOGGER = LoggerFactory.getLogger(SuspendRequestService.class);
    private volatile static SuspendRequestService instance;

    private final ConcurrentMap<Pair<String/*topic*/, Integer/*queueId*/>, ConcurrentSkipListSet<SuspendRequestTask<?>>> suspendPopRequestMap = new ConcurrentHashMap<>();
    private final AtomicInteger suspendRequestCount = new AtomicInteger(0);
    protected ThreadPoolExecutor suspendRequestThreadPool;

    private SuspendRequestService() {
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        this.suspendRequestThreadPool = ThreadPoolMonitor.createAndMonitor(
            config.getGrpcConsumerThreadPoolNums(),
            config.getGrpcConsumerThreadPoolNums(),
            1,
            TimeUnit.MINUTES,
            "SuspendRequestThreadPool",
            config.getGrpcConsumerThreadQueueCapacity()
        );
    }

    public static SuspendRequestService getInstance() {
        if (instance == null) {
            synchronized (SuspendRequestService.class) {
                if (instance == null) {
                    instance = new SuspendRequestService();
                }
            }
        }
        return instance;
    }

    @Override
    public String getServiceName() {
        return "SuspendPopRequestService";
    }

    public interface GetMessageResult {
        boolean needWriteResponse();
    }

    static class SuspendRequestTask<T extends GetMessageResult> implements Comparable<SuspendRequestTask<T>> {
        private final ProxyContextExt context;
        private final long bornTime;
        private final long timeLimit;
        private final Filter filter;
        private final Function<Long, CompletableFuture<T>> supplier;
        private final CompletableFuture<Optional<T>> future = new CompletableFuture<>();
        private final AtomicBoolean inflight = new AtomicBoolean(false);
        private final AtomicBoolean completed = new AtomicBoolean(false);

        public SuspendRequestTask(ProxyContextExt context, long timeLimit, Filter filter,
            Function<Long, CompletableFuture<T>> supplier) {
            this.context = context;
            this.bornTime = System.currentTimeMillis();
            this.timeLimit = timeLimit;
            this.filter = filter;
            this.supplier = supplier;
        }

        public long timeRemaining() {
            return bornTime + timeLimit - System.currentTimeMillis();
        }

        public CompletableFuture<Optional<T>> future() {
            return future;
        }

        public boolean doFilter(String tag) {
            return filter.doFilter(tag);
        }

        public boolean isExpired() {
            return System.currentTimeMillis() - bornTime > timeLimit;
        }

        public boolean completeTimeout() {
            if (inflight.compareAndSet(false, true)) {
                context.span().ifPresent(span -> span.setAttribute("result", "expired"));
                future.complete(Optional.empty());

                completed.set(true);
                inflight.set(false);
                return true;
            }
            return false;
        }

        public CompletableFuture<Boolean> tryFetchMessages() {
            // If the request is already completed, return immediately.
            if (completed.get()) {
                return CompletableFuture.completedFuture(true);
            }

            if (inflight.compareAndSet(false, true)) {
                // If the request is already expired, complete it immediately.
                if (isExpired()) {
                    context.span().ifPresent(span -> span.setAttribute("result", "expired"));
                    future.complete(Optional.empty());

                    completed.set(true);
                    inflight.set(false);
                    return CompletableFuture.completedFuture(true);
                }

                // Otherwise, fetch messages.
                return supplier.apply(timeRemaining())
                    .thenApply(result -> {
                        // If there are variable response to write back, return immediately.
                        if (result.needWriteResponse()) {
                            context.span().ifPresent(span -> span.setAttribute("result", "found"));
                            future.complete(Optional.of(result));

                            completed.set(true);
                            return true;
                        }
                        // Otherwise, wait for expire or notification.
                        return false;
                    })
                    .exceptionally(ex -> {
                        LOGGER.error("Error while fetching messages for suspended request.", ex);
                        context.span().ifPresent(span -> span.setAttribute("result", "error"));
                        future.completeExceptionally(ex);

                        completed.set(true);
                        return true;
                    })
                    .whenComplete((result, ex) -> inflight.set(false));
            }
            return CompletableFuture.completedFuture(false);
        }

        @Override
        public int compareTo(@Nonnull SuspendRequestService.SuspendRequestTask o) {
            int result = Long.compare(timeRemaining(), o.timeRemaining());
            if (result == 0) {
                // If the time remaining is the same, use the object identity to break the tie.
                return Integer.compare(System.identityHashCode(this), System.identityHashCode(o));
            }
            return result;
        }
    }

    public void notifyMessageArrival(String topic, int queueId, String tag) {
        ConcurrentSkipListSet<SuspendRequestTask<?>> taskList = suspendPopRequestMap.get(Pair.of(topic, queueId));
        if (taskList == null) {
            return;
        }

        for (SuspendRequestTask<?> task : taskList) {
            if (task.doFilter(tag)) {
                suspendRequestThreadPool.execute(
                    () -> task.tryFetchMessages()
                        .thenAccept(result -> {
                            if (result && taskList.remove(task)) {
                                suspendRequestCount.decrementAndGet();
                            }
                        }));
                break;
            }
        }
    }

    @WithSpan(kind = SpanKind.SERVER)
    public <T extends GetMessageResult> CompletableFuture<Optional<T>> suspendRequest(ProxyContextExt context,
        @SpanAttribute String topic, @SpanAttribute int queueId, @SpanAttribute Filter filter,
        @SpanAttribute long timeRemaining, Function<Long/*timeout*/, CompletableFuture<T>> supplier) {
        context.setSuspended(true);

        // TODO: make max size configurable.
        if (suspendRequestCount.get() > 1000) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        // Limit the suspend time to avoid timeout.
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        timeRemaining = timeRemaining - config.getGrpcClientConsumerMinLongPollingTimeoutMillis();

        // Check if the request is already expired.
        if (timeRemaining <= config.getGrpcClientConsumerMinLongPollingTimeoutMillis()) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        timeRemaining = Math.min(timeRemaining, config.getGrpcClientConsumerMaxLongPollingTimeoutMillis());

        SuspendRequestTask<T> task = new SuspendRequestTask<>(context, timeRemaining, filter, supplier);
        ConcurrentSkipListSet<SuspendRequestTask<?>> taskList = suspendPopRequestMap.computeIfAbsent(Pair.of(topic, queueId), k -> new ConcurrentSkipListSet<>());
        boolean added = taskList.add(task);

        // If the task cannot enqueue, return empty result.
        if (!added) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        suspendRequestCount.incrementAndGet();
        return task.future();
    }

    public int suspendRequestCount() {
        return suspendRequestCount.get();
    }

    protected void cleanExpiredRequest() {
        suspendPopRequestMap.forEach((topicQueueId, taskList) -> {
            for (SuspendRequestTask<?> task : taskList) {
                // Complete the request if it is expired.
                if (task.isExpired() && task.completeTimeout() && taskList.remove(task)) {
                    suspendRequestCount.decrementAndGet();
                }
            }
        });
    }

    @Override
    public void run() {
        while (!stopped) {
            waitForRunning(100);
            try {
                cleanExpiredRequest();
            } catch (Exception e) {
                LOGGER.error("Error while cleaning expired suspend pop request.", e);
            }
        }
    }
}
