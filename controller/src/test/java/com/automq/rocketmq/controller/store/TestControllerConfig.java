/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
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
