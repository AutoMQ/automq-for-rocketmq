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

    private int streamSetObjectCompactionInterval = 20;
    private long streamSetObjectCompactionCacheSize = 200 * 1024 * 1024;
    private int streamSetObjectCompactionUploadConcurrency = 8;
    private long streamSetObjectCompactionStreamSplitSize = 16 * 1024 * 1024;
    private int streamSetObjectCompactionForceSplitPeriod = 120;
    private int streamSetObjectCompactionMaxObjectNum = 500;
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

    public int streamSetObjectCompactionInterval() {
        return streamSetObjectCompactionInterval;
    }

    public long streamSetObjectCompactionCacheSize() {
        return streamSetObjectCompactionCacheSize;
    }

    public int streamSetObjectCompactionUploadConcurrency() {
        return streamSetObjectCompactionUploadConcurrency;
    }

    public long streamSetObjectCompactionStreamSplitSize() {
        return streamSetObjectCompactionStreamSplitSize;
    }

    public int streamSetObjectCompactionForceSplitPeriod() {
        return streamSetObjectCompactionForceSplitPeriod;
    }

    public int streamSetObjectCompactionMaxObjectNum() {
        return streamSetObjectCompactionMaxObjectNum;
    }

    public int streamSplitSizeThreshold() {
        return streamSplitSizeThreshold;
    }
}
