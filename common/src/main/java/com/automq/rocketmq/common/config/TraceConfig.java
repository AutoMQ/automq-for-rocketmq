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
public class TraceConfig {
    private boolean enabled = false;
    private String grpcExporterTarget = "";
    private long grpcExporterTimeOutInMills = 3 * 1000;
    private long periodicExporterIntervalInMills = 5 * 1000;

    // Maximum batch size for every export.
    // If batch size is zero, then every span is exported immediately.
    private int batchSize = 512;
    // Maximum number of Spans that are kept in the cache
    private int maxCachedSize = 2048;

    public boolean enabled() {
        return enabled;
    }

    public String grpcExporterTarget() {
        return grpcExporterTarget;
    }

    public long grpcExporterTimeOutInMills() {
        return grpcExporterTimeOutInMills;
    }

    public long periodicExporterIntervalInMills() {
        return periodicExporterIntervalInMills;
    }

    public int batchSize() {
        return batchSize;
    }

    public int maxCachedSize() {
        return maxCachedSize;
    }
}
