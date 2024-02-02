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

package com.automq.rocketmq.common.metrics;

public enum MetricsExporterType {
    DISABLE(0),
    OTLP_GRPC(1),
    PROM(2),
    LOG(3);

    private final int value;

    MetricsExporterType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static MetricsExporterType valueOf(int value) {
        switch (value) {
            case 1:
                return OTLP_GRPC;
            case 2:
                return PROM;
            case 3:
                return LOG;
            default:
                return DISABLE;
        }
    }

    public boolean isEnable() {
        return this.value > 0;
    }
}
