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

package com.automq.rocketmq.common.config;

import com.automq.rocketmq.common.exception.RocketMQException;
import org.apache.rocketmq.common.utils.NetworkUtil;

public class BrokerConfig implements ControllerConfig {
    /**
     * Node ID
     */
    private int id;

    private long epoch;

    private String name;

    /**
     * If the broker is running on an EC2 instance, this is the instance-id.
     */
    private String instanceId;

    /**
     * Sample bind address are:
     * 0.0.0.0:0
     * 0.0.0.0:8080
     * 10.0.0.1:0
     * 10.0.0.1:8081
     */
    private String bindAddress;

    /**
     * Advertise address in HOST:PORT format.
     */
    private String advertiseAddress;

    private final MetricsConfig metrics;
    private final ProxyConfig proxy;
    private final StoreConfig store;
    private final S3StreamConfig s3Stream;

    private final DatabaseConfig db;

    public BrokerConfig() {
        this.metrics = new MetricsConfig();
        this.proxy = new ProxyConfig();
        this.store = new StoreConfig();
        this.s3Stream = new S3StreamConfig();
        this.db = new DatabaseConfig();
    }

    private  static int parsePort(String address) {
        int pos = address.lastIndexOf(':');
        return Integer.parseInt(address.substring(pos + 1));
    }

    public void validate() throws RocketMQException {
        if (null == advertiseAddress) {
            String host = NetworkUtil.getLocalAddress();
            this.advertiseAddress = host + ":" + parsePort(bindAddress);
        }

        if (parsePort(advertiseAddress) != parsePort(bindAddress)) {
            throw new RocketMQException(500, "Listen port does not match advertise address port");
        }

        proxy.setGrpcListenPort(parsePort(advertiseAddress));
    }

    @Override
    public int nodeId() {
        return this.id;
    }

    @Override
    public void setNodeId(int nodeId) {
        this.id = nodeId;
    }

    @Override
    public long epoch() {
        return this.epoch;
    }

    @Override
    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    @Override
    public String dbUrl() {
        return this.db.getUrl();
    }

    @Override
    public String dbUserName() {
        return this.db.getUserName();
    }

    @Override
    public String dbPassword() {
        return this.db.getPassword();
    }

    public MetricsConfig metrics() {
        return metrics;
    }

    public ProxyConfig proxy() {
        return proxy;
    }

    public StoreConfig store() {
        return store;
    }

    public S3StreamConfig s3Stream() {
        return s3Stream;
    }

    public DatabaseConfig db() {
        return db;
    }

    public String name() {
        return name;
    }

    public String instanceId() {
        return instanceId;
    }

    public String advertiseAddress() {
        return advertiseAddress;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setBindAddress(String bindAddress) {
        this.bindAddress = bindAddress;
    }

    public void setAdvertiseAddress(String advertiseAddress) {
        this.advertiseAddress = advertiseAddress;
    }
}
