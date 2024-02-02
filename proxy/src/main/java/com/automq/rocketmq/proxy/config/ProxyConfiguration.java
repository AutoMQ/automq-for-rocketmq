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

package com.automq.rocketmq.proxy.config;

import com.automq.rocketmq.common.config.ProxyConfig;
import org.apache.rocketmq.proxy.config.Configuration;
import org.apache.rocketmq.proxy.config.ConfigurationManager;

public class ProxyConfiguration extends ConfigurationManager {
    public static void intConfig(ProxyConfig config) {
        configuration = new Configuration();

        org.apache.rocketmq.proxy.config.ProxyConfig innerConfig = new org.apache.rocketmq.proxy.config.ProxyConfig();
        innerConfig.setGrpcBossLoopNum(config.grpcBossLoopNum());
        innerConfig.setGrpcWorkerLoopNum(config.grpcWorkerLoopNum());
        innerConfig.setGrpcThreadPoolNums(config.grpcThreadPoolNums());
        innerConfig.setGrpcThreadPoolQueueCapacity(config.grpcThreadPoolQueueCapacity());
        innerConfig.setGrpcServerPort(config.getGrpcListenPort());
        innerConfig.setRemotingListenPort(config.remotingListenPort());
        innerConfig.setGrpcMaxInboundMessageSize(config.grpcMaxInboundMessageSize());
        innerConfig.setGrpcClientIdleTimeMills(config.grpcClientIdleTimeMills());
        innerConfig.setEnableGrpcEpoll(config.enableGrpcEpoll());

        configuration.setProxyConfig(innerConfig);
    }
}
