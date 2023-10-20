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

import apache.rocketmq.v2.ReceiveMessageRequest;
import com.automq.rocketmq.proxy.model.ProxyContextExt;
import com.automq.rocketmq.store.model.message.Filter;
import com.automq.rocketmq.store.model.message.SQLFilter;
import com.automq.rocketmq.store.model.message.TagFilter;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.grpc.v2.consumer.ReceiveMessageResponseStreamWriter;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SuspendPopRequestService extends ServiceThread implements StartAndShutdown {
    protected static final Logger LOGGER = LoggerFactory.getLogger(SuspendPopRequestService.class);
    private volatile static SuspendPopRequestService instance;

    private final ConcurrentMap<Pair<String/*topic*/, Integer/*queueId*/>, ConcurrentSkipListSet<SuspendRequestTask>> suspendPopRequestMap = new ConcurrentHashMap<>();
    private final AtomicInteger suspendRequestCount = new AtomicInteger(0);
    protected ThreadPoolExecutor suspendRequestThreadPool;

    private SuspendPopRequestService() {
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

    public static SuspendPopRequestService getInstance() {
        if (instance == null) {
            synchronized (SuspendPopRequestService.class) {
                if (instance == null) {
                    instance = new SuspendPopRequestService();
                }
            }
        }
        return instance;
    }

    @Override
    public String getServiceName() {
        return "SuspendPopRequestService";
    }

    static class SuspendRequestTask implements Comparable<SuspendRequestTask> {
        private final long bornTime;
        private final long timeLimit;
        private final Filter filter;
        private final Function<Long, CompletableFuture<PopResult>> supplier;
        private final Consumer<PopResult> writer;
        private final AtomicBoolean inflight = new AtomicBoolean(false);
        private final AtomicBoolean completed = new AtomicBoolean(false);

        public SuspendRequestTask(long timeLimit, Filter filter,
            Function<Long, CompletableFuture<PopResult>> supplier, Consumer<PopResult> writer) {
            this.bornTime = System.currentTimeMillis();
            this.timeLimit = timeLimit;
            this.filter = filter;
            this.supplier = supplier;
            this.writer = writer;
        }

        public long timeRemaining() {
            return bornTime + timeLimit - System.currentTimeMillis();
        }

        public boolean doFilter(String tag) {
            return filter.doFilter(tag);
        }

        public boolean isExpired() {
            return System.currentTimeMillis() - bornTime > timeLimit;
        }

        public boolean completeTimeout() {
            if (inflight.compareAndSet(false, true)) {
                completed.set(true);
                writer.accept(new PopResult(PopStatus.POLLING_NOT_FOUND, Collections.emptyList()));
                inflight.set(false);
                return true;
            }
            return false;
        }

        public CompletableFuture<Boolean> tryFetchMessages() {
            if (completed.get()) {
                return CompletableFuture.completedFuture(true);
            }

            if (inflight.compareAndSet(false, true)) {
                if (isExpired()) {
                    writer.accept(new PopResult(PopStatus.POLLING_NOT_FOUND, Collections.emptyList()));
                    completed.set(true);
                    inflight.set(false);
                    return CompletableFuture.completedFuture(true);
                }

                return supplier.apply(timeRemaining())
                    .thenApply(popResult -> {
                        if (popResult.getPopStatus() == PopStatus.FOUND) {
                            writer.accept(popResult);
                            completed.set(true);
                            return true;
                        }
                        inflight.set(false);
                        return false;
                    });
            }
            return CompletableFuture.completedFuture(false);
        }

        @Override
        public int compareTo(@Nonnull SuspendPopRequestService.SuspendRequestTask o) {
            return Long.compare(timeRemaining(), o.timeRemaining());
        }
    }

    public void notifyMessageArrival(String topic, int queueId, String tag) {
        ConcurrentSkipListSet<SuspendRequestTask> taskList = suspendPopRequestMap.get(Pair.of(topic, queueId));
        if (taskList == null) {
            return;
        }

        for (SuspendRequestTask task : taskList) {
            if (task.doFilter(tag)) {
                suspendRequestThreadPool.execute(
                    () -> task.tryFetchMessages()
                        .thenAccept(result -> {
                            if (result) {
                                taskList.remove(task);
                                suspendRequestCount.decrementAndGet();
                            }
                        }));
            }
        }
    }

    public void suspendPopRequest(ProxyContext context, ReceiveMessageRequest request, String topic, int queueId,
        SubscriptionData subscriptionData, long timeRemaining, Function<Long, CompletableFuture<PopResult>> supplier,
        ReceiveMessageResponseStreamWriter writer) {
        ((ProxyContextExt) context).setSuspended(true);

        // TODO: make max size configurable.
        if (suspendRequestCount.get() > 1000) {
            writer.writeAndComplete(context, request, new PopResult(PopStatus.POLLING_NOT_FOUND, Collections.emptyList()));
            return;
        }

        // Limit the suspend time to avoid timeout.
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        timeRemaining = timeRemaining - config.getGrpcClientConsumerMinLongPollingTimeoutMillis();

        // Check if the request is already expired.
        if (timeRemaining <= config.getGrpcClientConsumerMinLongPollingTimeoutMillis()) {
            writer.writeAndComplete(context, request, new PopResult(PopStatus.POLLING_NOT_FOUND, Collections.emptyList()));
            return;
        }

        timeRemaining = Math.min(timeRemaining, config.getGrpcClientConsumerMaxLongPollingTimeoutMillis());

        Filter filter;
        if (StringUtils.isNotBlank(subscriptionData.getExpressionType())) {
            filter = switch (subscriptionData.getExpressionType()) {
                case ExpressionType.TAG ->
                    subscriptionData.getSubString().contains(TagFilter.SUB_ALL) ? Filter.DEFAULT_FILTER : new TagFilter(subscriptionData.getSubString());
                case ExpressionType.SQL92 -> new SQLFilter(subscriptionData.getSubString());
                default -> Filter.DEFAULT_FILTER;
            };
        } else {
            filter = Filter.DEFAULT_FILTER;
        }

        SuspendRequestTask task = new SuspendRequestTask(timeRemaining, filter, supplier, popResult -> writer.writeAndComplete(context, request, popResult));
        ConcurrentSkipListSet<SuspendRequestTask> taskList = suspendPopRequestMap.computeIfAbsent(Pair.of(topic, queueId), k -> new ConcurrentSkipListSet<>());
        taskList.add(task);
        suspendRequestCount.incrementAndGet();
    }

    public int suspendRequestCount() {
        return suspendRequestCount.get();
    }

    protected void cleanExpiredRequest() {
        suspendPopRequestMap.forEach((topicQueueId, taskList) -> {
            for (SuspendRequestTask task : taskList) {
                // Complete the request if it is expired.
                if (task.isExpired() && task.completeTimeout()) {
                    taskList.remove(task);
                    suspendRequestCount.decrementAndGet();
                }
            }
        });
    }

    @Override
    public void run() {
        waitForRunning(100);

        while (!stopped) {
            try {
                cleanExpiredRequest();
            } catch (Exception e) {
                LOGGER.error("Error while cleaning expired suspend pop request.", e);
            }
        }
    }
}
