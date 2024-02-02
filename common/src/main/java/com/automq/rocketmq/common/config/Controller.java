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

public class Controller {
    private long recycleS3IntervalInSecs = 3600;

    private long scanIntervalInSecs = 30;

    private int leaseLifeSpanInSecs = 10;

    private long nodeAliveIntervalInSecs = 60;

    private int deletedTopicLingersInSecs = 300;

    private int deletedGroupLingersInSecs = 300;

    private long balanceWorkloadIntervalInSecs = 10;

    private int workloadTolerance = 2;

    /**
     * Flag whether system should dump heap on fatal error.
     */
    private boolean dumpHeapOnError = false;

    public long getRecycleS3IntervalInSecs() {
        return recycleS3IntervalInSecs;
    }

    public void setRecycleS3IntervalInSecs(long recycleS3IntervalInSecs) {
        this.recycleS3IntervalInSecs = recycleS3IntervalInSecs;
    }

    public long getScanIntervalInSecs() {
        return scanIntervalInSecs;
    }

    public void setScanIntervalInSecs(long scanIntervalInSecs) {
        this.scanIntervalInSecs = scanIntervalInSecs;
    }

    public int getLeaseLifeSpanInSecs() {
        return leaseLifeSpanInSecs;
    }

    public void setLeaseLifeSpanInSecs(int leaseLifeSpanInSecs) {
        this.leaseLifeSpanInSecs = leaseLifeSpanInSecs;
    }

    public long getNodeAliveIntervalInSecs() {
        return nodeAliveIntervalInSecs;
    }

    public void setNodeAliveIntervalInSecs(long nodeAliveIntervalInSecs) {
        this.nodeAliveIntervalInSecs = nodeAliveIntervalInSecs;
    }

    public int getDeletedTopicLingersInSecs() {
        return deletedTopicLingersInSecs;
    }

    public void setDeletedTopicLingersInSecs(int deletedTopicLingersInSecs) {
        this.deletedTopicLingersInSecs = deletedTopicLingersInSecs;
    }

    public int getDeletedGroupLingersInSecs() {
        return deletedGroupLingersInSecs;
    }

    public void setDeletedGroupLingersInSecs(int deletedGroupLingersInSecs) {
        this.deletedGroupLingersInSecs = deletedGroupLingersInSecs;
    }

    public long getBalanceWorkloadIntervalInSecs() {
        return balanceWorkloadIntervalInSecs;
    }

    public void setBalanceWorkloadIntervalInSecs(long balanceWorkloadIntervalInSecs) {
        this.balanceWorkloadIntervalInSecs = balanceWorkloadIntervalInSecs;
    }

    public int getWorkloadTolerance() {
        return workloadTolerance;
    }

    public void setWorkloadTolerance(int workloadTolerance) {
        this.workloadTolerance = workloadTolerance;
    }

    public boolean isDumpHeapOnError() {
        return dumpHeapOnError;
    }

    public void setDumpHeapOnError(boolean dumpHeapOnError) {
        this.dumpHeapOnError = dumpHeapOnError;
    }
}
