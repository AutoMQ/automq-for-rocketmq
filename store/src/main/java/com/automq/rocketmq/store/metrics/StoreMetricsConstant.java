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
package com.automq.rocketmq.store.metrics;

public class StoreMetricsConstant {
    public static final String GAUGE_CONSUMER_LAG_MESSAGES = "rocketmq_consumer_lag_messages";
    public static final String GAUGE_CONSUMER_LAG_LATENCY = "rocketmq_consumer_lag_latency";
    public static final String GAUGE_CONSUMER_INFLIGHT_MESSAGES = "rocketmq_consumer_inflight_messages";
    public static final String GAUGE_CONSUMER_QUEUEING_LATENCY = "rocketmq_consumer_queueing_latency";
    public static final String GAUGE_CONSUMER_READY_MESSAGES = "rocketmq_consumer_ready_messages";
    public static final String COUNTER_CONSUMER_SEND_TO_DLQ_MESSAGES_TOTAL = "rocketmq_send_to_dlq_messages_total";
    public static final String HISTOGRAM_STREAM_OPERATION_TIME = "rocketmq_stream_operation_latency";

    public static final String LABEL_TOPIC = "topic";
    public static final String LABEL_QUEUE_ID = "queue_id";
    public static final String LABEL_CONSUMER_GROUP = "consumer_group";
    public static final String LABEL_IS_RETRY = "is_retry";

}
