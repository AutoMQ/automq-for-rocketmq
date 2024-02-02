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

package com.automq.rocketmq.proxy.processor;

import apache.rocketmq.v2.Code;
import com.automq.rocketmq.common.config.ProxyConfig;
import com.automq.rocketmq.proxy.exception.ProxyException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.broker.client.ProducerManager;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.processor.DefaultMessagingProcessor;
import org.apache.rocketmq.proxy.processor.PopMessageResultFilter;
import org.apache.rocketmq.proxy.processor.QueueSelector;
import org.apache.rocketmq.proxy.service.ServiceManager;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

public class ExtendMessagingProcessor extends DefaultMessagingProcessor {
    private final ProxyConfig proxyConfig;

    protected ExtendMessagingProcessor(ServiceManager serviceManager, ProxyConfig proxyConfig) {
        super(serviceManager);
        this.proxyConfig = proxyConfig;
    }

    public static ExtendMessagingProcessor createForS3RocketMQ(ServiceManager serviceManager, ProxyConfig proxyConfig) {
        return new ExtendMessagingProcessor(serviceManager, proxyConfig);
    }

    @Override
    public CompletableFuture<PopResult> popMessage(ProxyContext ctx, QueueSelector queueSelector, String consumerGroup,
        String topic, int maxMsgNums, long invisibleTime, long pollTime, int initMode,
        SubscriptionData subscriptionData, boolean fifo, PopMessageResultFilter popMessageResultFilter,
        long timeoutMillis) {
        long timeout = timeoutMillis - proxyConfig.networkRTTMills();
        if (timeout < 0) {
            return CompletableFuture.failedFuture(new ProxyException(Code.ILLEGAL_POLLING_TIME, "timeout is too small"));
        }

        return super.popMessage(ctx, queueSelector, consumerGroup, topic, maxMsgNums, invisibleTime, pollTime, initMode, subscriptionData, fifo, popMessageResultFilter, timeoutMillis)
            .orTimeout(timeout, TimeUnit.MILLISECONDS);
    }

    public ServiceManager getServiceManager() {
        return serviceManager;
    }

    public ProducerManager producerManager() {
        return serviceManager.getProducerManager();
    }

    public ConsumerManager consumerManager() {
        return serviceManager.getConsumerManager();
    }
}
