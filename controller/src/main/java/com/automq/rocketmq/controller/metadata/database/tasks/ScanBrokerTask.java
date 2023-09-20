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

package com.automq.rocketmq.controller.metadata.database.tasks;

import com.automq.rocketmq.controller.metadata.database.DefaultMetadataStore;
import com.automq.rocketmq.controller.metadata.database.mapper.BrokerMapper;
import com.automq.rocketmq.controller.metadata.database.dao.Broker;
import java.util.List;
import org.apache.ibatis.session.SqlSession;

public class ScanBrokerTask extends ScanTask {

    public ScanBrokerTask(DefaultMetadataStore metadataStore) {
        super(metadataStore);
    }

    @Override
    public void run() {
        LOGGER.info("ScanBrokerTask starts");
        try {
            if (null == this.lastScanTime) {
                try (SqlSession session = this.metadataStore.getSessionFactory().openSession()) {
                    BrokerMapper mapper = session.getMapper(BrokerMapper.class);
                    List<Broker> brokers = mapper.list();
                    updateBrokers(brokers);
                }
            } else {
                try (SqlSession session = this.metadataStore.getSessionFactory().openSession()) {
                    BrokerMapper mapper = session.getMapper(BrokerMapper.class);
                    List<Broker> brokers = mapper.deltaList(this.lastScanTime);
                    updateBrokers(brokers);
                }
            }
        } catch (Throwable e) {
            LOGGER.error("Failed to scan brokers from database", e);
        }
        LOGGER.info("ScanBrokerTask completed");
    }

    private void updateBrokers(List<Broker> brokers) {
        if (null != brokers) {
            for (Broker broker : brokers) {
                this.metadataStore.addBroker(broker);
                if (null == this.lastScanTime) {
                    this.lastScanTime = broker.getUpdateTime();
                } else if (broker.getUpdateTime().after(this.lastScanTime)) {
                    this.lastScanTime = broker.getUpdateTime();
                }
            }
        }
    }
}
