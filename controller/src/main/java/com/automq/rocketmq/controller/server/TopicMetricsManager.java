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

package com.automq.rocketmq.controller.server;

import com.automq.rocketmq.common.MetricsManager;
import com.automq.rocketmq.controller.MetadataStore;
import com.automq.rocketmq.metadata.dao.QueueAssignment;
import com.automq.rocketmq.metadata.service.S3MetadataService;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class TopicMetricsManager implements MetricsManager {

    private static Supplier<AttributesBuilder> attributesBuilderSupplier;
    public static final String GAUGE_STORAGE_SIZE = "rocketmq_storage_size";
    public static final String GAUGE_STORAGE_MESSAGE_RESERVE_TIME = "rocketmq_storage_message_reserve_time";

    public static final String LABEL_TOPIC = "topic";
    public static final String LABEL_QUEUE = "queue";

    private final MetadataStore metadataStore;

    private final S3MetadataService s3MetadataService;

    private ObservableLongGauge storeGauge;

    private ObservableLongGauge reserveTimeGauge;

    public TopicMetricsManager(MetadataStore metadataStore, S3MetadataService s3MetadataService) {
        this.metadataStore = metadataStore;
        this.s3MetadataService = s3MetadataService;
    }

    @Override
    public void initAttributesBuilder(Supplier<AttributesBuilder> attributesBuilderSupplier) {
        TopicMetricsManager.attributesBuilderSupplier = attributesBuilderSupplier;
    }

    private void measureStoreSize(ObservableLongMeasurement measurement) {
        List<QueueAssignment> list = metadataStore.assignmentsOf(metadataStore.config().nodeId());
        for (QueueAssignment assignment : list) {
            List<Long> streamIds = metadataStore.streamsOf(assignment.getTopicId(), assignment.getQueueId());
            long total = 0;
            for (long id : streamIds) {
                total += s3MetadataService.streamDataSize(id);
            }
            Optional<String> topic = metadataStore.topicManager().nameOf(assignment.getTopicId());
            if (topic.isEmpty()) {
                continue;
            }
            measurement.record(total,
                attributesBuilderSupplier.get().put(LABEL_TOPIC, topic.get())
                    .put(LABEL_QUEUE, assignment.getQueueId()).build()
            );
        }
    }

    private void measureReserveTime(ObservableLongMeasurement measurement) {
        List<QueueAssignment> list = metadataStore.assignmentsOf(metadataStore.config().nodeId());
        long currentTs = System.currentTimeMillis();
        for (QueueAssignment assignment : list) {
            List<Long> streamIds = metadataStore.streamsOf(assignment.getTopicId(), assignment.getQueueId());
            long startTime = Long.MAX_VALUE;
            for (long id : streamIds) {
                long ts = s3MetadataService.streamStartTime(id);
                if (ts < startTime) {
                    startTime = ts;
                }
            }
            Optional<String> topic = metadataStore.topicManager().nameOf(assignment.getTopicId());
            if (topic.isEmpty()) {
                continue;
            }
            long reserveTime = 0;
            if (startTime < currentTs) {
                reserveTime = TimeUnit.MILLISECONDS.toSeconds(currentTs - startTime);
            }
            measurement.record(reserveTime,
                attributesBuilderSupplier.get().put(LABEL_TOPIC, topic.get())
                    .put(LABEL_QUEUE, assignment.getQueueId()).build()
            );
        }
    }

    @Override
    public void initStaticMetrics(Meter meter) {
        storeGauge = meter.gaugeBuilder(GAUGE_STORAGE_SIZE)
            .setUnit("byte")
            .ofLongs().setDescription("Storage size per topic/queue")
            .buildWithCallback(this::measureStoreSize);

        reserveTimeGauge = meter.gaugeBuilder(GAUGE_STORAGE_MESSAGE_RESERVE_TIME)
            .setUnit("Second")
            .ofLongs()
            .setDescription("Spanning duration of the reserved messages per topic/queue")
            .buildWithCallback(this::measureReserveTime);
    }

    @Override
    public void initDynamicMetrics(Meter meter) {
    }
}
