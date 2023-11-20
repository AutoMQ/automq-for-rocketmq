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
