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

public class Controller {
    private long recycleS3IntervalInSecs = 3600;

    private long scanIntervalInSecs = 30;

    private int leaseLifeSpanInSecs = 10;

    private long nodeAliveIntervalInSecs = 60;

    private int deletedTopicLingersInSecs = 300;

    private int deletedGroupLingersInSecs = 300;

    private long balanceWorkloadIntervalInSecs = 10;

    private int workloadTolerance = 2;

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
}
