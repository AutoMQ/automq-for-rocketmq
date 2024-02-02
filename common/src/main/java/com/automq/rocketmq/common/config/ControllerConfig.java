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

package com.automq.rocketmq.common.config;

public interface ControllerConfig extends GrpcClientConfig {
    int nodeId();

    /**
     * @return Node name
     */
    String name();

    void setNodeId(int nodeId);

    String instanceId();

    String advertiseAddress();

    long epoch();

    void setEpoch(long epoch);

    default long scanIntervalInSecs() {
        return 30;
    }

    default int leaseLifeSpanInSecs() {
        return 10;
    }

    default long nodeAliveIntervalInSecs() {
        return 60;
    }

    default int deletedTopicLingersInSecs() {
        return 300;
    }

    default int deletedGroupLingersInSecs() {
        return 300;
    }

    default long balanceWorkloadIntervalInSecs() {
        return 10;
    }

    default long recycleS3IntervalInSecs() {
        return 3600L;
    }

    /**
     * @return Tolerance of workload unfairness among nodes in terms of stream number.
     */
    default int workloadTolerance() {
        return 1;
    }

    String dbUrl();

    String dbUserName();

    String dbPassword();

    boolean goingAway();

    void flagGoingAway();

    default boolean dumpHeapOnError() {
        return true;
    }
}
