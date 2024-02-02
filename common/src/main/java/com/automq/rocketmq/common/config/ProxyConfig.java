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

package com.automq.rocketmq.common.config;

import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"FieldMayBeFinal", "FieldCanBeLocal"})
public class ProxyConfig extends BaseConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyConfig.class);

    private String name;

    private String hostName;

    // The proportion of messages that are popped from the retry queue first,
    // default is 20, available value from 0 to 100.
    private int retryPriorityPercentage = 20;

    // lock expire time, default is 15min, unit in milliseconds.
    private long lockExpireTime = Duration.ofMinutes(15).toMillis();

    private int grpcThreadPoolNums = 16 + PROCESSOR_NUMBER * 2;
    private int grpcThreadPoolQueueCapacity = 100000;
    private int grpcListenPort = 8081;
    private int remotingListenPort = 8080;
    private int grpcBossLoopNum = 1;
    private int grpcWorkerLoopNum = PROCESSOR_NUMBER * 2;
    private boolean enableGrpcEpoll = false;
    private long channelExpiredTimeout = 1000 * 120;
    private boolean enablePrintJstack = true;
    private long printJstackInMillis = Duration.ofSeconds(60).toMillis();
    private long printThreadPoolStatusInMillis = Duration.ofSeconds(3).toMillis();

    /**
     * gRPC max message size
     * 130M = 4M * 32 messages + 2M attributes
     */
    private int grpcMaxInboundMessageSize = 130 * 1024 * 1024;
    private long grpcClientIdleTimeMills = Duration.ofSeconds(120).toMillis();

    private long networkRTTMills = Duration.ofMillis(100).toMillis();

    public String name() {
        return name;
    }

    public String hostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
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

    public int grpcListenPort() {
        return grpcListenPort;
    }

    public boolean enablePrintJstack() {
        return enablePrintJstack;
    }

    public long printJstackInMillis() {
        return printJstackInMillis;
    }

    public long printThreadPoolStatusInMillis() {
        return printThreadPoolStatusInMillis;
    }

    public int remotingListenPort() {
        return remotingListenPort;
    }

    public void setRemotingListenPort(int remotingListenPort) {
        this.remotingListenPort = remotingListenPort;
    }

    public long networkRTTMills() {
        return networkRTTMills;
    }
}
