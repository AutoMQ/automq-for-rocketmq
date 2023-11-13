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
