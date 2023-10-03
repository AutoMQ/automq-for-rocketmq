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
import com.automq.rocketmq.metadata.api.ProxyMetadataService;
import com.automq.rocketmq.store.api.MessageStore;
import org.apache.rocketmq.broker.client.ConsumerGroupEvent;
import org.apache.rocketmq.broker.client.ConsumerIdsChangeListener;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.broker.client.ProducerManager;
import org.apache.rocketmq.proxy.service.ServiceManager;
import org.apache.rocketmq.proxy.service.admin.AdminService;
import org.apache.rocketmq.proxy.service.message.MessageService;
import org.apache.rocketmq.proxy.service.metadata.MetadataService;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.apache.rocketmq.proxy.service.route.TopicRouteService;
import org.apache.rocketmq.proxy.service.transaction.TransactionService;

public class DefaultServiceManager implements ServiceManager {
    private final ProxyMetadataService metadataService;
    private final TopicRouteService topicRouteService;
    private final MessageService messageService;
    private final MetadataService resourceMetadataService;
    private final ProducerManager producerManager;
    private final ConsumerManager consumerManager;

    public DefaultServiceManager(ProxyConfig config, ProxyMetadataService metadataService, MessageStore messageStore) {
        this.metadataService = metadataService;
        LockService lockService = new LockService(config);
        this.messageService = new MessageServiceImpl(config, messageStore, metadataService, lockService);
        this.resourceMetadataService = new ResourceMetadataService(metadataService);
        this.topicRouteService = new TopicRouteServiceImpl(metadataService);
        this.producerManager = new ProducerManager();
        this.consumerManager = new ConsumerManager(new ConsumerIdsChangeListenerImpl(), config.channelExpiredTimeout());
    }

    @Override
    public MessageService getMessageService() {
        return messageService;
    }

    @Override
    public TopicRouteService getTopicRouteService() {
        return topicRouteService;
    }

    @Override
    public ProducerManager getProducerManager() {
        return producerManager;
    }

    @Override
    public ConsumerManager getConsumerManager() {
        return consumerManager;
    }

    @Override
    public TransactionService getTransactionService() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public ProxyRelayService getProxyRelayService() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public MetadataService getMetadataService() {
        return resourceMetadataService;
    }

    @Override
    public AdminService getAdminService() {
        // We don't need this for the current design
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void shutdown() throws Exception {
    }

    @Override
    public void start() throws Exception {
    }

    protected static class ConsumerIdsChangeListenerImpl implements ConsumerIdsChangeListener {
        @Override
        public void handle(ConsumerGroupEvent event, String group, Object... args) {
        }
        @Override
        public void shutdown() {
        }
    }
}
