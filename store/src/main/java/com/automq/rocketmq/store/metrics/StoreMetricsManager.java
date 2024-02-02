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

import com.automq.rocketmq.common.MetricsManager;
import com.automq.rocketmq.common.ServiceThread;
import com.automq.rocketmq.common.config.MetricsConfig;
import com.automq.rocketmq.store.MessageStoreImpl;
import com.automq.rocketmq.store.api.LogicQueue;
import com.automq.rocketmq.store.api.StreamStore;
import com.automq.rocketmq.store.queue.DefaultLogicQueueManager;
import com.automq.rocketmq.store.queue.StreamLogicQueue;
import com.google.common.collect.Sets;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.View;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.common.metrics.NopLongCounter;
import org.apache.rocketmq.common.metrics.NopObservableLongGauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.automq.rocketmq.store.metrics.StoreMetricsConstant.GAUGE_CONSUMER_INFLIGHT_MESSAGES;
import static com.automq.rocketmq.store.metrics.StoreMetricsConstant.GAUGE_CONSUMER_LAG_LATENCY;
import static com.automq.rocketmq.store.metrics.StoreMetricsConstant.GAUGE_CONSUMER_LAG_MESSAGES;
import static com.automq.rocketmq.store.metrics.StoreMetricsConstant.GAUGE_CONSUMER_QUEUEING_LATENCY;
import static com.automq.rocketmq.store.metrics.StoreMetricsConstant.GAUGE_CONSUMER_READY_MESSAGES;
import static com.automq.rocketmq.store.metrics.StoreMetricsConstant.LABEL_CONSUMER_GROUP;
import static com.automq.rocketmq.store.metrics.StoreMetricsConstant.LABEL_IS_RETRY;
import static com.automq.rocketmq.store.metrics.StoreMetricsConstant.LABEL_QUEUE_ID;
import static com.automq.rocketmq.store.metrics.StoreMetricsConstant.LABEL_TOPIC;

public class StoreMetricsManager extends ServiceThread implements MetricsManager {
    protected static final Logger LOGGER = LoggerFactory.getLogger(StoreMetricsManager.class);

    public static ObservableLongGauge consumerLagMessages = new NopObservableLongGauge();

    public static ObservableLongGauge consumerLagLatency = new NopObservableLongGauge();
    public static ObservableLongGauge consumerInflightMessages = new NopObservableLongGauge();
    public static ObservableLongGauge consumerQueueingLatency = new NopObservableLongGauge();
    public static ObservableLongGauge consumerReadyMessages = new NopObservableLongGauge();

    // TODO: implement retry and dlq metrics
    public static LongCounter retryMessages = new NopLongCounter();
    public static LongCounter deadLetterMessages = new NopLongCounter();

    private static Supplier<AttributesBuilder> attributesBuilderSupplier;

    private final MetricsConfig config;
    private final MessageStoreImpl messageStore;
    private static Set<LagRecord> lagRecordSet = Sets.newConcurrentHashSet();

    public static AttributesBuilder newAttributesBuilder() {
        if (attributesBuilderSupplier == null) {
            return Attributes.builder();
        }
        return attributesBuilderSupplier.get();
    }

    public StoreMetricsManager(MetricsConfig config, MessageStoreImpl messageStore) {
        this.config = config;
        this.messageStore = messageStore;
    }

    @Override
    public String getServiceName() {
        return "StoreMetricsService";
    }

    @Override
    public void run() {
        while (!stopped) {
            waitForRunning(config.periodicExporterIntervalInMills());

            StreamStore streamStore = messageStore.streamStore();
            DefaultLogicQueueManager manager = (DefaultLogicQueueManager) messageStore.topicQueueManager();
            Set<LagRecord> newLagRecordSet = Sets.newConcurrentHashSet();
            manager.logicQueueMap().forEach((topicQueueId, logicQueueFuture) -> {
                if (!logicQueueFuture.isDone() || logicQueueFuture.isCompletedExceptionally()) {
                    return;
                }
                try {
                    StreamLogicQueue logicQueue = (StreamLogicQueue) logicQueueFuture.get();
                    if (logicQueue.getState() != LogicQueue.State.OPENED) {
                        return;
                    }

                    logicQueue.retryStreamIdMap().forEach((consumerGroupId, retryStreamIdFuture) -> {
                        long confirmOffset = streamStore.confirmOffset(logicQueue.dataStreamId());
                        long consumeOffset = logicQueue.getConsumeOffset(consumerGroupId);
                        int inflightCount = logicQueue.getInflightStats(consumerGroupId);
                        // TODO: build lag record for retry stream
                        LagRecord record = new LagRecord(logicQueue.topicId(), logicQueue.queueId(), consumerGroupId, false,
                            confirmOffset - consumeOffset + inflightCount, 0, inflightCount, 0, confirmOffset - consumeOffset);
                        newLagRecordSet.add(record);
                    });
                } catch (Exception e) {
                    LOGGER.error("Failed to update metrics for logic queue {}", topicQueueId, e);
                }
            });
            lagRecordSet = newLagRecordSet;
        }
    }

    private Attributes buildLagAttributes(LagRecord record) {
        AttributesBuilder attributesBuilder = newAttributesBuilder();
        attributesBuilder.put(LABEL_CONSUMER_GROUP, record.consumerGroupId());
        attributesBuilder.put(LABEL_TOPIC, record.topicId());
        attributesBuilder.put(LABEL_QUEUE_ID, record.queueId());
        attributesBuilder.put(LABEL_IS_RETRY, record.retry());
        return attributesBuilder.build();
    }

    @Override
    public void initAttributesBuilder(Supplier<AttributesBuilder> attributesBuilderSupplier) {
        StoreMetricsManager.attributesBuilderSupplier = attributesBuilderSupplier;
    }

    @Override
    public void initStaticMetrics(Meter meter) {

    }

    @Override
    public void initDynamicMetrics(Meter meter) {
        consumerLagMessages = meter.gaugeBuilder(GAUGE_CONSUMER_LAG_MESSAGES)
            .setDescription("Consumer lag messages")
            .ofLongs()
            .buildWithCallback(measurement -> lagRecordSet.forEach(record -> measurement.record(record.lag(), buildLagAttributes(record))));

        consumerLagLatency = meter.gaugeBuilder(GAUGE_CONSUMER_LAG_LATENCY)
            .setDescription("Consumer lag time")
            .setUnit("milliseconds")
            .ofLongs()
            .buildWithCallback(measurement -> lagRecordSet.forEach(record -> measurement.record(record.LagLatency(), buildLagAttributes(record))));

        consumerInflightMessages = meter.gaugeBuilder(GAUGE_CONSUMER_INFLIGHT_MESSAGES)
            .setDescription("Consumer inflight messages")
            .ofLongs()
            .buildWithCallback(measurement -> lagRecordSet.forEach(record -> measurement.record(record.inflight(), buildLagAttributes(record))));

        consumerQueueingLatency = meter.gaugeBuilder(GAUGE_CONSUMER_QUEUEING_LATENCY)
            .setDescription("Consumer queueing time")
            .setUnit("milliseconds")
            .ofLongs()
            .buildWithCallback(measurement -> lagRecordSet.forEach(record -> measurement.record(record.queueingLatency(), buildLagAttributes(record))));

        consumerReadyMessages = meter.gaugeBuilder(GAUGE_CONSUMER_READY_MESSAGES)
            .setDescription("Consumer ready messages")
            .ofLongs()
            .buildWithCallback(measurement -> lagRecordSet.forEach(record -> measurement.record(record.ready(), buildLagAttributes(record))));
    }

    public static List<Pair<InstrumentSelector, View>> getMetricsView() {
        return Collections.emptyList();
    }
}
