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

package com.automq.rocketmq.broker;

import com.automq.rocketmq.broker.protocol.GrpcProtocolServer;
import com.automq.rocketmq.common.config.BrokerConfig;
import com.automq.rocketmq.common.util.Lifecycle;
import com.automq.rocketmq.metadata.ProxyMetadataService;
import com.automq.rocketmq.metadata.StoreMetadataService;
import com.automq.rocketmq.store.api.MessageStore;
import com.automq.rocketmq.store.api.StreamStore;
import org.apache.rocketmq.proxy.service.ServiceManager;

public class BrokerController implements Lifecycle {
    private final BrokerConfig brokerConfig;
    private final StreamStore streamStore = null;
    private final ServiceManager serviceManager = null;
    private final GrpcProtocolServer grpcServer;
    private final MessageStore messageStore = null;
    private final StoreMetadataService storeMetadataService = null;
    private final ProxyMetadataService proxyMetadataService = null;

    public BrokerController(BrokerConfig brokerConfig) {
        this.brokerConfig = brokerConfig;
        this.grpcServer = new GrpcProtocolServer(brokerConfig.proxy(), null);
    }

    @Override
    public void start() throws Exception {
        this.grpcServer.start();
    }

    @Override
    public void shutdown() throws Exception {
        this.grpcServer.shutdown();
    }
}
