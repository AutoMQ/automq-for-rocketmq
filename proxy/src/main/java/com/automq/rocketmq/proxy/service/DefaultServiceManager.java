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

import com.automq.rocketmq.common.config.BrokerConfig;
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
    private final ProxyRelayService proxyRelayService;
    private final TransactionService transactionService;
    private final AdminService adminService;
    private final DeadLetterService deadLetterService;

    public DefaultServiceManager(BrokerConfig config, ProxyMetadataService proxyMetadataService,
        DeadLetterService deadLetterService, MessageService messageService,
        MessageStore messageStore) {
        this.metadataService = proxyMetadataService;
        this.deadLetterService = deadLetterService;
        this.resourceMetadataService = new ResourceMetadataService(proxyMetadataService);
        this.messageService = messageService;
        this.topicRouteService = new TopicRouteServiceImpl(config, proxyMetadataService);
        this.producerManager = new ProducerManager();
        this.consumerManager = new ConsumerManager(new ConsumerIdsChangeListenerImpl(), config.proxy().channelExpiredTimeout());
        this.proxyRelayService = new ProxyRelayServiceImpl();
        this.transactionService = new TransactionServiceImpl();
        this.adminService = new AdminServiceImpl();
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
        return transactionService;
    }

    @Override
    public ProxyRelayService getProxyRelayService() {
        return proxyRelayService;
    }

    @Override
    public MetadataService getMetadataService() {
        return resourceMetadataService;
    }

    @Override
    public AdminService getAdminService() {
        // We don't need this for the current design
        return adminService;
    }

    @Override
    public void shutdown() throws Exception {
        topicRouteService.shutdown();
    }

    @Override
    public void start() throws Exception {
    }

    protected static class ConsumerIdsChangeListenerImpl implements ConsumerIdsChangeListener {
        @Override
        public void handle(ConsumerGroupEvent event, String group, Object... args) {
            // TODO: implement this to support consumer group change notification
        }
        @Override
        public void shutdown() {
        }
    }
}
