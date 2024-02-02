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
public class MetricsConfig {
    private String exporterType = "DISABLE";

    private String grpcExporterTarget = "";
    private String grpcExporterHeader = "";
    private long grpcExporterTimeOutInMills = 3 * 1000;
    private long periodicExporterIntervalInMills = 60 * 1000;

    private int promExporterPort = 5557;
    private String promExporterHost = "localhost";

    // Label pairs in CSV. Each label follows pattern of Key:Value. eg: instance_id:xxx,uid:xxx
    private String labels = "";

    private boolean exportInDelta = false;

    private boolean exportJVMMetrics = false;
    private boolean exportSystemMetrics = false;

    public String exporterType() {
        return exporterType;
    }

    public String grpcExporterTarget() {
        return grpcExporterTarget;
    }

    public String grpcExporterHeader() {
        return grpcExporterHeader;
    }

    public long grpcExporterTimeOutInMills() {
        return grpcExporterTimeOutInMills;
    }

    public long periodicExporterIntervalInMills() {
        return periodicExporterIntervalInMills;
    }

    public int promExporterPort() {
        return promExporterPort;
    }

    public String promExporterHost() {
        return promExporterHost;
    }

    public String labels() {
        return labels;
    }

    public boolean exportInDelta() {
        return exportInDelta;
    }

    public boolean exportJVMMetrics() {
        return exportJVMMetrics;
    }

    public boolean exportSystemMetrics() {
        return exportSystemMetrics;
    }
}
