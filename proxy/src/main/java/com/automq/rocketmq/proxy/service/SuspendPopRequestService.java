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

import com.automq.rocketmq.common.model.FlatMessageExt;
import com.automq.rocketmq.proxy.model.ProxyContextExt;
import com.automq.rocketmq.proxy.util.FlatMessageUtil;
import com.automq.rocketmq.store.model.message.Filter;
import com.automq.rocketmq.store.model.message.TopicQueueId;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SuspendPopRequestService extends ServiceThread {
    protected static final Logger LOGGER = LoggerFactory.getLogger(SuspendPopRequestService.class);

    private final ConcurrentMap<TopicQueueId, ConcurrentSkipListSet<SuspendRequestTask>> suspendPopRequestMap = new ConcurrentHashMap<>();
    private final AtomicInteger suspendRequestCount = new AtomicInteger(0);

    @Override
    public String getServiceName() {
        return "SuspendPopRequestService";
    }

    static class SuspendRequestTask implements Comparable<SuspendRequestTask> {
        private final PopMessageRequestHeader requestHeader;
        private final Filter filter;
        private final Supplier<CompletableFuture<List<FlatMessageExt>>> messageSupplier;
        private final CompletableFuture<PopResult> future;
        private final AtomicBoolean inflight = new AtomicBoolean(false);

        public SuspendRequestTask(PopMessageRequestHeader requestHeader, Filter filter,
            Supplier<CompletableFuture<List<FlatMessageExt>>> messageSupplier) {
            this.requestHeader = requestHeader;
            this.filter = filter;
            this.messageSupplier = messageSupplier;
            this.future = new CompletableFuture<>();
        }

        public long bornTime() {
            return requestHeader.getBornTime();
        }

        public CompletableFuture<PopResult> future() {
            return future;
        }

        public boolean doFilter(String tag) {
            return filter.doFilter(tag);
        }

        public boolean isExpired() {
            return requestHeader.getBornTime() + requestHeader.getPollTime() < System.currentTimeMillis();
        }

        public CompletableFuture<Boolean> tryFetchMessages() {
            if (future.isDone()) {
                return CompletableFuture.completedFuture(true);
            }

            if (inflight.compareAndSet(false, true)) {
                return messageSupplier.get()
                    .thenApply(messageList -> {
                        if (!messageList.isEmpty()) {
                            future.complete(new PopResult(PopStatus.FOUND, FlatMessageUtil.convertTo(messageList, requestHeader.getTopic(), requestHeader.getInvisibleTime())));
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
            return Long.compare(bornTime(), o.bornTime());
        }
    }

    public void notifyMessageArrival(long topicId, int queueId, String tag) {
        ConcurrentSkipListSet<SuspendRequestTask> taskList = suspendPopRequestMap.get(new TopicQueueId(topicId, queueId));
        if (taskList == null) {
            return;
        }

        for (SuspendRequestTask task : taskList) {
            if (task.doFilter(tag)) {
                task.tryFetchMessages()
                    .thenAccept(result -> {
                        if (result) {
                            taskList.remove(task);
                            suspendRequestCount.decrementAndGet();
                        }
                    });
            }
        }
    }

    public CompletableFuture<PopResult> suspendPopRequest(ProxyContext context, PopMessageRequestHeader requestHeader,
        long topicId, int queueId, Filter filter, Supplier<CompletableFuture<List<FlatMessageExt>>> messageSupplier) {
        ((ProxyContextExt) context).setSuspended(true);

        // Check if the request is already expired.
        if (requestHeader.getPollTime() <= 0) {
            return CompletableFuture.completedFuture(new PopResult(PopStatus.NO_NEW_MSG, Collections.emptyList()));
        }

        if (requestHeader.getBornTime() + requestHeader.getPollTime() <= System.currentTimeMillis()) {
            return CompletableFuture.completedFuture(new PopResult(PopStatus.NO_NEW_MSG, Collections.emptyList()));
        }

        // TODO: make max size configurable.
        if (suspendRequestCount.get() > 1000) {
            return CompletableFuture.completedFuture(new PopResult(PopStatus.POLLING_FULL, Collections.emptyList()));
        }

        SuspendRequestTask task = new SuspendRequestTask(requestHeader, filter, messageSupplier);
        ConcurrentSkipListSet<SuspendRequestTask> taskList = suspendPopRequestMap.computeIfAbsent(new TopicQueueId(topicId, queueId), k -> new ConcurrentSkipListSet<>());
        taskList.add(task);
        suspendRequestCount.incrementAndGet();
        return task.future();
    }

    public int suspendRequestCount() {
        return suspendRequestCount.get();
    }

    protected void cleanExpiredRequest() {
        suspendPopRequestMap.forEach((topicQueueId, taskList) -> {
            for (SuspendRequestTask task : taskList) {
                // Complete the request if it is expired.
                if (task.isExpired()) {
                    task.future().complete(new PopResult(PopStatus.NO_NEW_MSG, Collections.emptyList()));
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
