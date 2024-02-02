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

@SuppressWarnings({"FieldMayBeFinal", "FieldCanBeLocal"})
public class StoreConfig {
    private static final int PROCESSOR_NUMBER = Runtime.getRuntime().availableProcessors();

    // Fetch limitation of a single request.
    // Filtered out messages will also be counted.

    // Default is 1000
    private int maxFetchCount = 1000;

    // Default is 10MB, unit in bytes
    private long maxFetchBytes = 10L * 1024 * 1024;

    // Default is 10s, unit in milliseconds
    private long maxFetchTimeMillis = 10L * 1000;

    private int fetchBatchSizeFactor = 2;

    // Used for storing KV data based on RocksDB
    private String kvPath = "/tmp/s3rocketmq/kvstore";

    // DEFAULT is 1000 * 1000, unit in records
    private int operationSnapshotInterval = 1000 * 1000;

    private int workingThreadPoolNums = PROCESSOR_NUMBER;
    private int workingThreadQueueCapacity = 10000;

    private long transactionTimeoutMillis = 6 * 1000;

    private long transactionCheckInterval = 30 * 1000;

    private int transactionCheckMaxTimes = 15;

    public int maxFetchCount() {
        return maxFetchCount;
    }

    public long maxFetchBytes() {
        return maxFetchBytes;
    }

    public long maxFetchTimeMillis() {
        return maxFetchTimeMillis;
    }

    public int fetchBatchSizeFactor() {
        return fetchBatchSizeFactor;
    }

    public String kvPath() {
        return kvPath;
    }

    public int operationSnapshotInterval() {
        return operationSnapshotInterval;
    }

    public void setOperationSnapshotInterval(int operationSnapshotInterval) {
        this.operationSnapshotInterval = operationSnapshotInterval;
    }

    public int workingThreadPoolNums() {
        return workingThreadPoolNums;
    }

    public void setWorkingThreadPoolNums(int workingThreadPoolNums) {
        this.workingThreadPoolNums = workingThreadPoolNums;
    }

    public int workingThreadQueueCapacity() {
        return workingThreadQueueCapacity;
    }

    public void setWorkingThreadQueueCapacity(int workingThreadQueueCapacity) {
        this.workingThreadQueueCapacity = workingThreadQueueCapacity;
    }

    public long transactionTimeoutMillis() {
        return transactionTimeoutMillis;
    }

    public long transactionCheckInterval() {
        return transactionCheckInterval;
    }

    public int transactionCheckMaxTimes() {
        return transactionCheckMaxTimes;
    }
}
