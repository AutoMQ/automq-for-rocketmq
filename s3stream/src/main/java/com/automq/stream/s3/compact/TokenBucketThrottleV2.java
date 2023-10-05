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

package com.automq.stream.s3.compact;

import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.commons.lang3.tuple.Pair;

public class TokenBucketThrottleV2 {
    private final long tokenSize;

    private Queue<Pair<Long, CompletableFuture<Void>>> queue;


    private final ScheduledExecutorService executorService;

    public TokenBucketThrottleV2(long tokenSize) {
        this.tokenSize = tokenSize;
        this.queue = new java.util.concurrent.ConcurrentLinkedQueue<>();
        this.executorService = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("token-bucket-throttle"));
        this.executorService.scheduleWithFixedDelay(() -> {
            long remainingSize = tokenSize;
            while(!queue.isEmpty() && remainingSize > 0) {
                Pair<Long, CompletableFuture<Void>> pair = queue.peek();
                if (pair.getLeft() <= remainingSize) {
                    pair.getRight().complete(null);
                    queue.poll();
                }
                // minus the size regardless of whether the task is completed or not
                remainingSize -= pair.getLeft();
            }
        }, 0, 1, java.util.concurrent.TimeUnit.SECONDS);
    }

    public void stop() {
        this.executorService.shutdown();
        while(!queue.isEmpty()) {
            Pair<Long, CompletableFuture<Void>> pair = queue.poll();
            pair.getRight().completeExceptionally(new InterruptedException("TokenBucketThrottle shutdown"));
        }
    }

    public long getTokenSize() {
        return tokenSize;
    }

    public CompletableFuture<Void> throttle(long size) {
        if (size > tokenSize) {
            return CompletableFuture.failedFuture(new RuntimeException(String.format("Requesting size %d is bigger than token size %d", size, tokenSize)));
        }
        CompletableFuture<Void> future = new CompletableFuture<>();
        queue.offer(Pair.of(size, future));
        return future;
    }
}
