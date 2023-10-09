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

package com.automq.rocketmq.common.config;

import java.time.Duration;

public class ProxyConfig extends BaseConfig {
    private String name;

    // The proportion of messages that are popped from the retry queue first,
    // default is 20, available value from 0 to 100.
    private int retryPriorityPercentage = 20;

    // lock expire time, default is 15min, unit in nanoseconds.
    private long lockExpireTime = Duration.ofMinutes(15).toNanos();

    private int grpcThreadPoolNums = 16 + PROCESSOR_NUMBER * 2;
    private int grpcThreadPoolQueueCapacity = 100000;
    private int grpcListenPort = 8081;
    private int grpcBossLoopNum = 1;
    private int grpcWorkerLoopNum = PROCESSOR_NUMBER * 2;
    private boolean enableGrpcEpoll = false;
    private long channelExpiredTimeout = 1000 * 120;

    /**
     * gRPC max message size
     * 130M = 4M * 32 messages + 2M attributes
     */
    private int grpcMaxInboundMessageSize = 130 * 1024 * 1024;
    private long grpcClientIdleTimeMills = Duration.ofSeconds(120).toMillis();

    public String name() {
        return name;
    }

    public int retryPriorityPercentage() {
        return retryPriorityPercentage;
    }

    public long lockExpireTime() {
        return lockExpireTime;
    }

    public int grpcThreadPoolNums() {
        return grpcThreadPoolNums;
    }

    public int grpcThreadPoolQueueCapacity() {
        return grpcThreadPoolQueueCapacity;
    }

    public int getGrpcListenPort() {
        return grpcListenPort;
    }

    public void setGrpcListenPort(int grpcListenPort) {
        this.grpcListenPort = grpcListenPort;
    }

    public int grpcBossLoopNum() {
        return grpcBossLoopNum;
    }

    public int grpcWorkerLoopNum() {
        return grpcWorkerLoopNum;
    }

    public boolean enableGrpcEpoll() {
        return enableGrpcEpoll;
    }

    public int grpcMaxInboundMessageSize() {
        return grpcMaxInboundMessageSize;
    }

    public long grpcClientIdleTimeMills() {
        return grpcClientIdleTimeMills;
    }

    public long channelExpiredTimeout() {
        return channelExpiredTimeout;
    }
}
