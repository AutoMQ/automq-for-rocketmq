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

package com.automq.rocketmq.controller.metadata;

import com.automq.rocketmq.controller.metadata.database.mapper.BrokerMapper;
import com.automq.rocketmq.controller.metadata.database.dao.Broker;
import java.io.IOException;
import java.util.List;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class BrokerTest extends DatabaseTestBase {

    @Test
    @Order(1)
    public void testListBrokers() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            BrokerMapper brokerMapper = session.getMapper(BrokerMapper.class);
            List<Broker> list = brokerMapper.list();
            Assertions.assertTrue(list.isEmpty());
        }
    }

    @Test
    @Order(2)
    public void testBroker_CRUD() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            BrokerMapper brokerMapper = session.getMapper(BrokerMapper.class);
            Broker broker = new Broker();
            broker.setName("Test-1");
            broker.setAddress("localhost:1234");
            broker.setInstanceId("i-asdf");
            int affectedRows = brokerMapper.create(broker);

            Assertions.assertEquals(1, affectedRows);

            List<Broker> brokers = brokerMapper.list();
            Assertions.assertEquals(1, brokers.size());
            Assertions.assertEquals("Test-1", brokers.get(0).getName());
            Assertions.assertEquals("i-asdf", brokers.get(0).getInstanceId());
            Assertions.assertEquals("localhost:1234", brokers.get(0).getAddress());

            brokerMapper.delete(broker.getId());

            brokers = brokerMapper.list();
            Assertions.assertTrue(brokers.isEmpty());
        }
    }

    @Test
    @Order(3)
    public void testTerm() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            BrokerMapper brokerMapper = session.getMapper(BrokerMapper.class);
            Broker broker = new Broker();
            broker.setName("Test-1");
            broker.setAddress("localhost:1234");
            broker.setInstanceId("i-asdf");
            int affectedRows = brokerMapper.create(broker);
            Assertions.assertEquals(1, affectedRows);

            Broker broker1 = brokerMapper.getByInstanceId(broker.getInstanceId());
            Assertions.assertEquals(1, broker1.getEpoch());
            Assertions.assertEquals(broker.getId(), broker1.getId());
            affectedRows = brokerMapper.increaseEpoch(broker.getId());
            Assertions.assertEquals(1, affectedRows);

            broker1 = brokerMapper.getByInstanceId(broker1.getInstanceId());
            Assertions.assertEquals(2, broker1.getEpoch());
            brokerMapper.delete(affectedRows);
        }
    }

}
