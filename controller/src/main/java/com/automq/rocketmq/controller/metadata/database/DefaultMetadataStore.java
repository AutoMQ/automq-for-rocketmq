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

import apache.rocketmq.controller.v1.Code;
import com.automq.rocketmq.common.PrefixThreadFactory;
import com.automq.rocketmq.controller.exception.ControllerException;
import com.automq.rocketmq.controller.metadata.ControllerClient;
import com.automq.rocketmq.controller.metadata.ControllerConfig;
import com.automq.rocketmq.controller.metadata.MetadataStore;
import com.automq.rocketmq.controller.metadata.Role;
import com.automq.rocketmq.controller.metadata.database.mapper.BrokerMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.LeaseMapper;
import com.automq.rocketmq.controller.metadata.database.dao.Broker;
import com.automq.rocketmq.controller.metadata.database.dao.Lease;
import com.automq.rocketmq.controller.metadata.database.tasks.LeaseTask;
import com.automq.rocketmq.controller.metadata.database.tasks.ScanBrokerTask;
import com.google.common.base.Strings;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultMetadataStore implements MetadataStore, Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultMetadataStore.class);

    private final ControllerClient controllerClient;

    private final SqlSessionFactory sessionFactory;

    private final ControllerConfig config;

    private Role role;

    private final ConcurrentHashMap<Integer, Broker> brokers;

    private final ScheduledExecutorService executorService;

    /// The following fields are runtime specific
    private Lease lease;

    public DefaultMetadataStore(ControllerClient client, SqlSessionFactory sessionFactory, ControllerConfig config) {
        controllerClient = client;
        this.sessionFactory = sessionFactory;
        this.config = config;
        this.role = Role.Follower;
        this.brokers = new ConcurrentHashMap<>();
        this.executorService = new ScheduledThreadPoolExecutor(Runtime.getRuntime().availableProcessors(),
            new PrefixThreadFactory("Controller"));

    }

    public void start() {
        this.executorService.scheduleAtFixedRate(new LeaseTask(this), 1, config.scanIntervalInSecs(), TimeUnit.SECONDS);
        this.executorService.scheduleWithFixedDelay(new ScanBrokerTask(this), 1, config.scanIntervalInSecs(), TimeUnit.SECONDS);
        LOGGER.info("MetadataStore tasks scheduled");
    }

    @Override
    public Broker registerBroker(String name, String address, String instanceId) throws ControllerException {
        if (Strings.isNullOrEmpty(name)) {
            throw new ControllerException(Code.BAD_REQUEST_VALUE, "Broker name is null or empty");
        }

        if (Strings.isNullOrEmpty(address)) {
            throw new ControllerException(Code.BAD_REQUEST_VALUE, "Broker address is null or empty");
        }

        if (Strings.isNullOrEmpty(instanceId)) {
            throw new ControllerException(Code.BAD_REQUEST_VALUE, "Broker instance-id is null or empty");
        }

        for (;;) {
            if (this.isLeader()) {
                try (SqlSession session = this.sessionFactory.openSession(false)) {
                    BrokerMapper brokerMapper = session.getMapper(BrokerMapper.class);
                    Broker broker = brokerMapper.getByInstanceId(instanceId);
                    if (null != broker) {
                        brokerMapper.increaseEpoch(broker.getId());
                        broker.setEpoch(broker.getEpoch() + 1);
                        return broker;
                    } else {
                        LeaseMapper leaseMapper = session.getMapper(LeaseMapper.class);
                        Lease lease = leaseMapper.currentWithShareLock();
                        if (lease.getEpoch() != this.lease.getEpoch()) {
                            this.lease = lease;
                            LOGGER.info("Controller has yielded its leader role");
                            continue;
                        }
                        broker = new Broker();
                        broker.setName(name);
                        broker.setAddress(address);
                        broker.setInstanceId(instanceId);
                        brokerMapper.create(broker);
                        session.commit();
                        return broker;
                    }
                }
            } else {
                return controllerClient.registerBroker(this.leaderAddress(), name, address, instanceId);
            }
        }
    }

    @Override
    public boolean isLeader() throws ControllerException {
        return this.role == Role.Leader;
    }

    @Override
    public String leaderAddress() throws ControllerException {
        if (null == lease || lease.expired()) {
            LOGGER.error("No lease is populated yet or lease was expired");
            throw new ControllerException(Code.NO_LEADER_VALUE);
        }

        Broker broker = brokers.get(this.lease.getBrokerId());
        if (null == broker) {
            LOGGER.error("Address for Broker with brokerId={} is missing", this.lease.getBrokerId());
            throw new ControllerException(Code.NOT_FOUND_VALUE,
                String.format("Broker is unexpected missing with brokerId=%d", this.lease.getBrokerId()));
        }

        return broker.getAddress();
    }

    @Override
    public void close() throws IOException {
        this.executorService.shutdown();
    }

    public void setRole(Role role) {
        this.role = role;
    }

    public void setLease(Lease lease) {
        this.lease = lease;
    }

    public Lease getLease() {
        return lease;
    }

    public void addBroker(Broker broker) {
        this.brokers.put(broker.getId(), broker);
    }

    public ConcurrentMap<Integer, Broker> getBrokers() {
        return brokers;
    }

    public SqlSessionFactory getSessionFactory() {
        return sessionFactory;
    }

    public ControllerConfig getConfig() {
        return config;
    }
}
