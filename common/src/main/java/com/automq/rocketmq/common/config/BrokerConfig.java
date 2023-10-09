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

import org.apache.rocketmq.common.utils.NetworkUtil;

public class BrokerConfig implements ControllerConfig {
    private String name;

    /**
     * If the broker is running on an EC2 instance, this is the instance-id.
     */
    private String instanceId;

    private String address = NetworkUtil.getLocalAddress();
    private final ProxyConfig proxy;
    private final StoreConfig store;
    private final S3StreamConfig s3Stream;

    private final DatabaseConfig db;

    public BrokerConfig() {
        this.proxy = new ProxyConfig();
        this.store = new StoreConfig();
        this.s3Stream = new S3StreamConfig();
        this.db = new DatabaseConfig();
    }

    @Override
    public int nodeId() {
        return 0;
    }

    @Override
    public long epoch() {
        return 0;
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

    public ProxyConfig proxy() {
        return proxy;
    }

    public StoreConfig store() {
        return store;
    }

    public S3StreamConfig s3Stream() {
        return s3Stream;
    }

    public DatabaseConfig getDb() {
        return db;
    }

    public String name() {
        return name;
    }

    public String instanceId() {
        return instanceId;
    }

    public String address() {
        return address;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setAddress(String address) {
        this.address = address;
    }
}
