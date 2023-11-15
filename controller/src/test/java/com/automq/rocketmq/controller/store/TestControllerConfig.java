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

package com.automq.rocketmq.controller.store;

import com.automq.rocketmq.common.config.ControllerConfig;

public class TestControllerConfig implements ControllerConfig {
    int nodeId;

    String name = "UnitTestNode";

    String instanceId = "UnitTestInstance";

    String advertiseAddress = "localhost:1234";

    long epoch;

    boolean goingAway;

    String dbUrl;
    String dbUserName;
    String dbPassword;

    @Override
    public int nodeId() {
        return nodeId;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public String instanceId() {
        return instanceId;
    }

    @Override
    public String advertiseAddress() {
        return advertiseAddress;
    }

    @Override
    public long epoch() {
        return epoch;
    }

    @Override
    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    @Override
    public String dbUrl() {
        return dbUrl;
    }

    @Override
    public String dbUserName() {
        return dbUserName;
    }

    @Override
    public String dbPassword() {
        return dbPassword;
    }

    public void setDbUrl(String dbUrl) {
        this.dbUrl = dbUrl;
    }

    public void setDbUserName(String dbUserName) {
        this.dbUserName = dbUserName;
    }

    public void setDbPassword(String dbPassword) {
        this.dbPassword = dbPassword;
    }

    @Override
    public boolean goingAway() {
        return goingAway;
    }

    @Override
    public void flagGoingAway() {
        this.goingAway = true;
    }
}
