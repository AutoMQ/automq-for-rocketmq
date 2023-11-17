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
package com.automq.rocketmq.store.metrics;

public class StoreMetricsConstant {
    public static final String GAUGE_CONSUMER_LAG_MESSAGES = "rocketmq_consumer_lag_messages";
    public static final String GAUGE_CONSUMER_LAG_LATENCY = "rocketmq_consumer_lag_latency";
    public static final String GAUGE_CONSUMER_INFLIGHT_MESSAGES = "rocketmq_consumer_inflight_messages";
    public static final String GAUGE_CONSUMER_QUEUEING_LATENCY = "rocketmq_consumer_queueing_latency";
    public static final String GAUGE_CONSUMER_READY_MESSAGES = "rocketmq_consumer_ready_messages";
    public static final String COUNTER_CONSUMER_SEND_TO_DLQ_MESSAGES_TOTAL = "rocketmq_send_to_dlq_messages_total";
    public static final String HISTOGRAM_STREAM_OPERATION_TIME = "rocketmq_stream_operation_time";

    public static final String LABEL_TOPIC = "topic";
    public static final String LABEL_QUEUE_ID = "queue_id";
    public static final String LABEL_CONSUMER_GROUP = "consumer_group";
    public static final String LABEL_IS_RETRY = "is_retry";

}
