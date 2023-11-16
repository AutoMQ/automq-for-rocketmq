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
