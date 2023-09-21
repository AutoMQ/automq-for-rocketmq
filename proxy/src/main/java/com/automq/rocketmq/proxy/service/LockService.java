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

import com.automq.rocketmq.common.config.ProxyConfig;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.StringUtils;

public class LockService {
    private final ConcurrentMap<Long /* topicId */, ConcurrentMap<Integer /* queueId */, Lock>> lockMap = new ConcurrentHashMap<>();

    private final ProxyConfig config;

    public LockService(ProxyConfig config) {
        this.config = config;
    }

    private Lock findLock(long topicId, int queueId) {
        return lockMap.computeIfAbsent(topicId, v -> new ConcurrentHashMap<>())
            .computeIfAbsent(queueId, v -> new Lock(topicId, queueId));
    }

    public boolean tryLock(long topicId, int queueId, String clientId, boolean fifo) {
        Lock lock = findLock(topicId, queueId);
        if (fifo) {
            return lock.tryPreempt(clientId, config.lockExpireTime());
        }
        return lock.tryLock(clientId);
    }

    public void release(long topicId, int queueId) {
        findLock(topicId, queueId)
            .release();
    }

    public void tryExpire(long topicId, int queueId, long later) {
        findLock(topicId, queueId)
            .tryExpire(later, config.lockExpireTime());
    }

    private static class Lock {
        private final long topicId;
        private final int queueId;

        private final AtomicBoolean processing = new AtomicBoolean(false);
        private final AtomicBoolean preempting = new AtomicBoolean(false);
        private volatile long lockTime;
        private volatile String ownerId = "";

        public Lock(long topicId, int queueId) {
            this.topicId = topicId;
            this.queueId = queueId;
        }

        public boolean tryLock(String clientId) {
            boolean result = processing.compareAndSet(false, true);
            if (result) {
                this.lockTime = System.nanoTime();
                this.ownerId = clientId;
                return true;
            }
            return false;
        }

        public void release() {
            processing.set(false);
        }

        // Used in FIFO mode, only the same client can acquire the lock or preempt the expired lock.
        public boolean tryPreempt(String clientId, long expireTime) {
            boolean result = preempting.compareAndSet(false, true);
            if (result) {
                try {
                    if (StringUtils.isBlank(this.ownerId) || this.ownerId.equals(clientId)) {
                        // Only same clientId could acquire the lock.
                        return tryLock(clientId);
                    } else if (lockTime + expireTime < System.nanoTime()) {
                        // Try to preempt expired lock.
                        this.lockTime = System.nanoTime();
                        this.ownerId = clientId;
                        this.processing.set(true);
                        return true;
                    }
                    return false;
                } finally {
                    preempting.set(false);
                }
            }
            return false;
        }

        // Used in FIFO mode, expire the lock after the specified time.
        public void tryExpire(long later, long expireTime) {
            if (preempting.compareAndSet(false, true)) {
                try {
                    // If there is ongoing pop request, skip expiring lock.
                    if (processing.get()) {
                        return;
                    }

                    lockTime = System.nanoTime() + later - expireTime;
                } finally {
                    preempting.set(false);
                }
            }
        }

        @Override
        public String toString() {
            return "Lock{" +
                "topicId=" + topicId +
                ", queueId=" + queueId +
                ", processing=" + processing +
                '}';
        }
    }
}
