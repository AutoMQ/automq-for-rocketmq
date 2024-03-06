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

package com.automq.stream.s3.metrics;

import io.opentelemetry.api.common.AttributeKey;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class S3StreamMetricsConstant {
    // value = 16KB * 2^i
    public static final String[] OBJECT_SIZE_BUCKET_NAMES = {
        "16KB",
        "32KB",
        "64KB",
        "128KB",
        "256KB",
        "512KB",
        "1MB",
        "2MB",
        "4MB",
        "8MB",
        "16MB",
        "32MB",
        "64MB",
        "128MB",
        "inf"};
    public static final List<Long> LATENCY_BOUNDARIES = List.of(
        TimeUnit.MICROSECONDS.toNanos(1),
        TimeUnit.MICROSECONDS.toNanos(10),
        TimeUnit.MICROSECONDS.toNanos(100),
        TimeUnit.MILLISECONDS.toNanos(1),
        TimeUnit.MILLISECONDS.toNanos(3),
        TimeUnit.MILLISECONDS.toNanos(5),
        TimeUnit.MILLISECONDS.toNanos(7),
        TimeUnit.MILLISECONDS.toNanos(10),
        TimeUnit.MILLISECONDS.toNanos(20),
        TimeUnit.MILLISECONDS.toNanos(30),
        TimeUnit.MILLISECONDS.toNanos(40),
        TimeUnit.MILLISECONDS.toNanos(50),
        TimeUnit.MILLISECONDS.toNanos(60),
        TimeUnit.MILLISECONDS.toNanos(70),
        TimeUnit.MILLISECONDS.toNanos(80),
        TimeUnit.MILLISECONDS.toNanos(90),
        TimeUnit.MILLISECONDS.toNanos(100),
        TimeUnit.MILLISECONDS.toNanos(200),
        TimeUnit.MILLISECONDS.toNanos(500),
        TimeUnit.SECONDS.toNanos(1),
        TimeUnit.SECONDS.toNanos(3),
        TimeUnit.SECONDS.toNanos(5),
        TimeUnit.SECONDS.toNanos(10),
        TimeUnit.SECONDS.toNanos(30),
        TimeUnit.MINUTES.toNanos(1),
        TimeUnit.MINUTES.toNanos(3),
        TimeUnit.MINUTES.toNanos(5)
    );

    public static final String UPLOAD_SIZE_METRIC_NAME = "upload_size";
    public static final String DOWNLOAD_SIZE_METRIC_NAME = "download_size";
    public static final String OPERATION_COUNT_METRIC_NAME = "operation_count";
    public static final String OPERATION_LATENCY_METRIC_NAME = "operation_latency";
    public static final String OBJECT_COUNT_METRIC_NAME = "object_count";
    public static final String OBJECT_STAGE_COST_METRIC_NAME = "object_stage_cost";
    public static final String OBJECT_UPLOAD_SIZE_METRIC_NAME = "object_upload_size";
    public static final String NETWORK_INBOUND_USAGE_METRIC_NAME = "network_inbound_usage";
    public static final String NETWORK_OUTBOUND_USAGE_METRIC_NAME = "network_outbound_usage";
    public static final String NETWORK_INBOUND_AVAILABLE_BANDWIDTH_METRIC_NAME = "network_inbound_available_bandwidth";
    public static final String NETWORK_OUTBOUND_AVAILABLE_BANDWIDTH_METRIC_NAME = "network_outbound_available_bandwidth";
    public static final String NETWORK_INBOUND_LIMITER_QUEUE_SIZE_METRIC_NAME = "network_inbound_limiter_queue_size";
    public static final String NETWORK_OUTBOUND_LIMITER_QUEUE_SIZE_METRIC_NAME = "network_outbound_limiter_queue_size";
    public static final String NETWORK_INBOUND_LIMITER_QUEUE_TIME_METRIC_NAME = "network_inbound_limiter_queue_time";
    public static final String NETWORK_OUTBOUND_LIMITER_QUEUE_TIME_METRIC_NAME = "network_outbound_limiter_queue_time";
    public static final String READ_AHEAD_SIZE_METRIC_NAME = "read_ahead_size";
    public static final String SUM_METRIC_NAME_SUFFIX = "_sum";
    public static final String COUNT_METRIC_NAME_SUFFIX = "_count";
    public static final String P50_METRIC_NAME_SUFFIX = "_50p";
    public static final String P99_METRIC_NAME_SUFFIX = "_99p";
    public static final String MEAN_METRIC_NAME_SUFFIX = "_mean";
    public static final String MAX_METRIC_NAME_SUFFIX = "_max";
    public static final String WAL_START_OFFSET = "wal_start_offset";
    public static final String WAL_TRIMMED_OFFSET = "wal_trimmed_offset";
    public static final String DELTA_WAL_CACHE_SIZE = "delta_wal_cache_size";
    public static final String BLOCK_CACHE_SIZE = "block_cache_size";
    public static final String AVAILABLE_INFLIGHT_READ_AHEAD_SIZE_METRIC_NAME = "available_inflight_read_ahead_size";
    public static final String READ_AHEAD_QUEUE_TIME_METRIC_NAME = "read_ahead_limiter_queue_time";
    public static final String AVAILABLE_S3_INFLIGHT_READ_QUOTA_METRIC_NAME = "available_s3_inflight_read_quota";
    public static final String AVAILABLE_S3_INFLIGHT_WRITE_QUOTA_METRIC_NAME = "available_s3_inflight_write_quota";
    public static final String INFLIGHT_WAL_UPLOAD_TASKS_COUNT_METRIC_NAME = "inflight_wal_upload_tasks_count";
    public static final String COMPACTION_READ_SIZE_METRIC_NAME = "compaction_read_size";
    public static final String COMPACTION_WRITE_SIZE_METRIC_NAME = "compaction_write_size";
    public static final String BUFFER_ALLOCATED_MEMORY_SIZE_METRIC_NAME = "buffer_allocated_memory_size";
    public static final String BUFFER_USED_MEMORY_SIZE_METRIC_NAME = "buffer_used_memory_size";
    public static final AttributeKey<String> LABEL_OPERATION_TYPE = AttributeKey.stringKey("operation_type");
    public static final AttributeKey<String> LABEL_OPERATION_NAME = AttributeKey.stringKey("operation_name");
    public static final AttributeKey<String> LABEL_SIZE_NAME = AttributeKey.stringKey("size");
    public static final AttributeKey<String> LABEL_STAGE = AttributeKey.stringKey("stage");
    public static final AttributeKey<String> LABEL_STATUS = AttributeKey.stringKey("status");
    public static final AttributeKey<String> LABEL_ALLOC_TYPE = AttributeKey.stringKey("type");
    public static final String LABEL_STATUS_SUCCESS = "success";
    public static final String LABEL_STATUS_FAILED = "failed";
    public static final String LABEL_STATUS_HIT = "hit";
    public static final String LABEL_STATUS_MISS = "miss";
    public static final String LABEL_STATUS_SYNC = "sync";
    public static final String LABEL_STATUS_ASYNC = "async";

}
