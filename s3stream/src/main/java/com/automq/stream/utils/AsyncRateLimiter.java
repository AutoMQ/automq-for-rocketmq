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

package com.automq.stream.utils;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("UnstableApiUsage")
public class AsyncRateLimiter {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncRateLimiter.class);
    private static final ScheduledExecutorService scheduler = Threads.newSingleThreadScheduledExecutor("async-rate-limiter", true, LOGGER);
    private final Queue<Acquire> acquireQueue = new ConcurrentLinkedQueue<>();
    private final RateLimiter rateLimiter;
    private final ScheduledFuture<?> tickTask;

    public AsyncRateLimiter(double bytesPerSec) {
        rateLimiter = RateLimiter.create(bytesPerSec, 100, TimeUnit.MILLISECONDS);
        tickTask = scheduler.scheduleAtFixedRate(this::tick, 1, 1, TimeUnit.MILLISECONDS);
    }

    public synchronized CompletableFuture<Void> acquire(int size) {
        if (acquireQueue.isEmpty() && rateLimiter.tryAcquire(size)) {
            return CompletableFuture.completedFuture(null);
        } else {
            CompletableFuture<Void> cf = new CompletableFuture<>();
            acquireQueue.add(new Acquire(cf, size));
            return cf;
        }
    }

    public void close() {
        tickTask.cancel(false);
    }

    private synchronized void tick() {
        for (; ; ) {
            Acquire acquire = acquireQueue.peek();
            if (acquire == null) {
                break;
            }
            if (rateLimiter.tryAcquire(acquire.size)) {
                acquireQueue.poll();
                acquire.cf.complete(null);
            } else {
                break;
            }
        }
    }

    record Acquire(CompletableFuture<Void> cf, int size) {
    }

}
