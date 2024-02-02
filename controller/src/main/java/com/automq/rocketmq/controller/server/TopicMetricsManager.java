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
            long startTime = currentTs;
            for (long id : streamIds) {
                long ts = s3MetadataService.streamStartTime(id);
                if (ts < startTime && ts > 0) {
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
