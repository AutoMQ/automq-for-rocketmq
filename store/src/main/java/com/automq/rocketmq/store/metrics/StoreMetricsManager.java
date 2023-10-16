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
import org.apache.rocketmq.common.ServiceThread;
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
import static com.automq.rocketmq.store.metrics.StoreMetricsConstant.LABEL_TOPIC;

public class StoreMetricsManager extends ServiceThread {
    protected static final Logger LOGGER = LoggerFactory.getLogger(StoreMetricsManager.class);

    public static ObservableLongGauge consumerLagMessages = new NopObservableLongGauge();

    // TODO: implement consumerLagLatency
    public static ObservableLongGauge consumerLagLatency = new NopObservableLongGauge();
    public static ObservableLongGauge consumerInflightMessages = new NopObservableLongGauge();
    // TODO: implement consumerQueueingLatency
    public static ObservableLongGauge consumerQueueingLatency = new NopObservableLongGauge();
    public static ObservableLongGauge consumerReadyMessages = new NopObservableLongGauge();

    // TODO: implement retry and dlq metrics
    public static LongCounter retryMessages = new NopLongCounter();
    public static LongCounter deadLetterMessages = new NopLongCounter();

    private static Supplier<AttributesBuilder> attributesBuilderSupplier;

    private final MessageStoreImpl messageStore;
    private static Set<LagRecord> lagRecordSet = Sets.newConcurrentHashSet();

    public static AttributesBuilder newAttributesBuilder() {
        if (attributesBuilderSupplier == null) {
            return Attributes.builder();
        }
        return attributesBuilderSupplier.get();
    }

    public StoreMetricsManager(MessageStoreImpl messageStore) {
        this.messageStore = messageStore;
    }

    @Override
    public String getServiceName() {
        return "StoreMetricsService";
    }

    @Override
    public void run() {
        while (!stopped) {
            waitForRunning(30 * 1000);

            StreamStore streamStore = messageStore.streamStore();
            DefaultLogicQueueManager manager = (DefaultLogicQueueManager) messageStore.getTopicQueueManager();
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
                        long consumeOffset = logicQueue.getRetryConsumeOffset(consumerGroupId);
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
        attributesBuilder.put(LABEL_IS_RETRY, record.retry());
        return attributesBuilder.build();
    }

    public void initMetrics(Meter meter, Supplier<AttributesBuilder> attributesBuilderSupplier) {
        StoreMetricsManager.attributesBuilderSupplier = attributesBuilderSupplier;
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
