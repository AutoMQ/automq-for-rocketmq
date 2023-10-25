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

public interface ControllerConfig {
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
        return 60;
    }

    default int nodeAliveIntervalInSecs() {
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

}
