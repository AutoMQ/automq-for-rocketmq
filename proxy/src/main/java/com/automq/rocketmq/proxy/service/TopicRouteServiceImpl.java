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

import com.automq.rocketmq.metadata.ProxyMetadataService;
import java.util.List;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.proxy.common.Address;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;
import org.apache.rocketmq.proxy.service.route.MessageQueueView;
import org.apache.rocketmq.proxy.service.route.ProxyTopicRouteData;
import org.apache.rocketmq.proxy.service.route.TopicRouteService;

public class TopicRouteServiceImpl extends TopicRouteService {
    private final ProxyMetadataService metadataService;

    public TopicRouteServiceImpl(ProxyMetadataService metadataService) {
        // We don't need MQClientAPIFactory anymore, so just pass a null.
        super(null);
        this.metadataService = metadataService;
    }

    @Override
    protected void init() {
        this.appendShutdown(this.scheduledExecutorService::shutdown);
    }

    @Override
    public MessageQueueView getCurrentMessageQueueView(ProxyContext ctx, String topicName) throws Exception {
        return null;
    }

    @Override
    public ProxyTopicRouteData getTopicRouteForProxy(ProxyContext ctx, List<Address> requestHostAndPortList,
        String topicName) throws Exception {
        return null;
    }

    @Override
    public String getBrokerAddr(ProxyContext ctx, String brokerName) throws Exception {
        return null;
    }

    @Override
    public AddressableMessageQueue buildAddressableMessageQueue(ProxyContext ctx,
        MessageQueue messageQueue) throws Exception {
        return null;
    }
}