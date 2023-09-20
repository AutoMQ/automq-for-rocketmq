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

package com.automq.rocketmq.controller.metadata.database;

import com.automq.rocketmq.controller.exception.ControllerException;
import com.automq.rocketmq.controller.metadata.ControllerClient;
import com.automq.rocketmq.controller.metadata.ControllerConfig;
import com.automq.rocketmq.controller.metadata.DatabaseTestBase;
import com.automq.rocketmq.controller.metadata.database.mapper.BrokerMapper;
import com.automq.rocketmq.controller.metadata.database.dao.Broker;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.ibatis.session.SqlSession;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class DefaultMetadataStoreTest extends DatabaseTestBase {

    ControllerClient client;

    public DefaultMetadataStoreTest() {
        this.client = Mockito.mock(ControllerClient.class);
    }

    @Test
    void testRegisterBroker() throws ControllerException, IOException {
        ControllerConfig config = Mockito.mock(ControllerConfig.class);
        Mockito.when(config.brokerId()).thenReturn(1);
        Mockito.when(config.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(config.leaseLifeSpanInSecs()).thenReturn(1);
        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().atMost(10, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::isLeader);

            String name = "broker-0";
            String address = "localhost:1234";
            String instanceId = "i-register";
            Broker broker = metadataStore.registerBroker(name, address, instanceId);
            Assertions.assertTrue(broker.getId() > 0);
            try (SqlSession session = getSessionFactory().openSession()) {
                BrokerMapper brokerMapper = session.getMapper(BrokerMapper.class);
                brokerMapper.delete(broker.getId());
            }
        }
    }

    @Test
    void testRegisterBroker_badArguments() throws ControllerException, IOException {
        ControllerConfig config = Mockito.mock(ControllerConfig.class);
        Mockito.when(config.brokerId()).thenReturn(1);
        Mockito.when(config.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(config.leaseLifeSpanInSecs()).thenReturn(1);
        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await().with().atMost(10, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .until(metadataStore::isLeader);
            String name = "test-broker -0";
            String address = "localhost:1234";
            String instanceId = "i-register";
            Assertions.assertThrows(ControllerException.class, () -> metadataStore.registerBroker("", address, instanceId));
            Assertions.assertThrows(ControllerException.class, () -> metadataStore.registerBroker(name, null, instanceId));
            Assertions.assertThrows(ControllerException.class, () -> metadataStore.registerBroker(name, address, ""));
        }
    }

    /**
     * Dummy test, should be removed later
     *
     * @throws IOException
     */
    @Test
    void testGetLease() throws IOException {
        ControllerConfig config = Mockito.mock(ControllerConfig.class);
        Mockito.when(config.brokerId()).thenReturn(1);
        Mockito.when(config.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(config.leaseLifeSpanInSecs()).thenReturn(1);
        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            Assertions.assertNull(metadataStore.getLease());
        }
    }

    @Test
    void testIsLeader() throws IOException {
        ControllerConfig config = Mockito.mock(ControllerConfig.class);
        Mockito.when(config.brokerId()).thenReturn(1);
        Mockito.when(config.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(config.leaseLifeSpanInSecs()).thenReturn(1);
        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await()
                .with()
                .atMost(10, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS).until(metadataStore::isLeader);
        }
    }

    @Test
    void testLeaderAddress() throws IOException, ControllerException {
        String address = "localhost:1234";
        int brokerId;
        try (SqlSession session = getSessionFactory().openSession()) {
            BrokerMapper brokerMapper = session.getMapper(BrokerMapper.class);
            Broker broker = new Broker();
            broker.setAddress(address);
            broker.setName("broker-test-name");
            broker.setInstanceId("i-leader-address");
            brokerMapper.create(broker);
            brokerId = broker.getId();
            session.commit();
        }

        ControllerConfig config = Mockito.mock(ControllerConfig.class);
        Mockito.when(config.brokerId()).thenReturn(brokerId);
        Mockito.when(config.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(config.leaseLifeSpanInSecs()).thenReturn(1);

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await()
                .with()
                .atMost(10, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS).until(metadataStore::isLeader);

            Awaitility.await()
                .with()
                .atMost(10, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS).until(() -> !metadataStore.getBrokers().isEmpty());

            Assertions.assertEquals(metadataStore.getLease().getBrokerId(), brokerId);

            String addr = metadataStore.leaderAddress();
            Assertions.assertEquals(address, addr);
        }

        try (SqlSession session = getSessionFactory().openSession(true)) {
            BrokerMapper brokerMapper = session.getMapper(BrokerMapper.class);
            brokerMapper.delete(brokerId);
        }
    }

    @Test
    void testLeaderAddress_NoLeader() throws IOException, ControllerException {
        ControllerConfig config = Mockito.mock(ControllerConfig.class);
        Mockito.when(config.brokerId()).thenReturn(1);
        Mockito.when(config.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(config.leaseLifeSpanInSecs()).thenReturn(1);
        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            Assertions.assertThrows(ControllerException.class, metadataStore::leaderAddress);
        }
    }

    @Test
    void testLeaderAddress_NoLeaderBroker() throws IOException, ControllerException {
        ControllerConfig config = Mockito.mock(ControllerConfig.class);
        Mockito.when(config.brokerId()).thenReturn(1);
        Mockito.when(config.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(config.leaseLifeSpanInSecs()).thenReturn(1);
        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            Awaitility.await()
                .with()
                .atMost(10, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS).until(metadataStore::isLeader);

            Assertions.assertThrows(ControllerException.class, metadataStore::leaderAddress);
        }
    }
}