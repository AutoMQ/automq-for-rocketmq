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
public class S3StreamConfig {
    private String s3Endpoint;
    private String s3Region = "cn-hangzhou";
    private String s3Bucket;
    private boolean s3ForcePathStyle;
    private String s3WALPath = "/tmp/s3stream_wal";
    private String s3AccessKey;
    private String s3SecretKey;
    private String s3Namespace = "S3_ROCKETMQ";

    // Max bandwidth in bytes per refillPeriodMs, default to 0 which means no limit.
    private long networkBaselineBandwidth = 0;
    private int refillPeriodMs = 1000;

    private int streamObjectCompactionIntervalMinutes = 60;
    private long streamObjectCompactionMaxSizeBytes = 10737418240L;
    private int streamObjectCompactionLivingTimeMinutes = 60;

    private int objectBlockSize = 1048576;

    // Cache
    private int walCacheSize = 1024 * 1024 * 1024;
    private int blockCacheSize = 1024 * 1024 * 1024;

    private int walObjectCompactionInterval = 20;
    private long walObjectCompactionCacheSize = 200 * 1024 * 1024;
    private int walObjectCompactionUploadConcurrency = 8;
    private long walObjectCompactionStreamSplitSize = 16 * 1024 * 1024;
    private int walObjectCompactionForceSplitPeriod = 120;
    private int walObjectCompactionMaxObjectNum = 500;
    private int streamSplitSizeThreshold = 16777216;

    public String s3Endpoint() {
        return s3Endpoint;
    }

    public String s3Region() {
        return s3Region;
    }

    public String s3Bucket() {
        return s3Bucket;
    }

    public boolean s3ForcePathStyle() {
        return s3ForcePathStyle;
    }

    public String s3WALPath() {
        return s3WALPath;
    }

    public String s3AccessKey() {
        return s3AccessKey;
    }

    public String s3SecretKey() {
        return s3SecretKey;
    }

    public long networkBaselineBandwidth() {
        return networkBaselineBandwidth;
    }

    public int refillPeriodMs() {
        return refillPeriodMs;
    }

    public String s3Namespace() {
        return s3Namespace;
    }

    public int streamObjectCompactionIntervalMinutes() {
        return streamObjectCompactionIntervalMinutes;
    }

    public long streamObjectCompactionMaxSizeBytes() {
        return streamObjectCompactionMaxSizeBytes;
    }

    public int streamObjectCompactionLivingTimeMinutes() {
        return streamObjectCompactionLivingTimeMinutes;
    }

    public int objectBlockSize() {
        return objectBlockSize;
    }

    public int walCacheSize() {
        return walCacheSize;
    }

    public int blockCacheSize() {
        return blockCacheSize;
    }

    public int walObjectCompactionInterval() {
        return walObjectCompactionInterval;
    }

    public long walObjectCompactionCacheSize() {
        return walObjectCompactionCacheSize;
    }

    public int walObjectCompactionUploadConcurrency() {
        return walObjectCompactionUploadConcurrency;
    }

    public long walObjectCompactionStreamSplitSize() {
        return walObjectCompactionStreamSplitSize;
    }

    public int walObjectCompactionForceSplitPeriod() {
        return walObjectCompactionForceSplitPeriod;
    }

    public int walObjectCompactionMaxObjectNum() {
        return walObjectCompactionMaxObjectNum;
    }

    public int streamSplitSizeThreshold() {
        return streamSplitSizeThreshold;
    }
}
