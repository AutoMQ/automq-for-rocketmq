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

package com.automq.rocketmq.proxy.metrics;

import com.automq.rocketmq.common.MetricsManager;
import com.automq.rocketmq.common.metrics.NopLongCounter;
import com.automq.rocketmq.common.metrics.NopLongHistogram;
import com.automq.rocketmq.common.metrics.NopObservableLongGauge;
import com.automq.rocketmq.proxy.processor.ExtendMessagingProcessor;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import io.opentelemetry.sdk.metrics.Aggregation;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.View;
import io.opentelemetry.sdk.metrics.ViewBuilder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.topic.TopicValidator;

import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.COUNTER_MESSAGES_IN_TOTAL;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.COUNTER_MESSAGES_OUT_TOTAL;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.COUNTER_THROUGHPUT_IN_TOTAL;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.COUNTER_THROUGHPUT_OUT_TOTAL;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.GAUGE_CONSUMER_CONNECTIONS;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.GAUGE_PRODUCER_CONNECTIONS;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.HISTOGRAM_MESSAGE_SIZE;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_CONSUMER_GROUP;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_CONSUME_MODE;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_IS_RETRY;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_IS_SYSTEM;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_LANGUAGE;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_MESSAGE_TYPE;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_TOPIC;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_VERSION;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.HISTOGRAM_RPC_LATENCY;

public class ProxyMetricsManager implements MetricsManager {
    public static final String LABEL_PROTOCOL_TYPE = "protocol_type";
    public static final String LABEL_ACTION = "action";
    public static final String LABEL_RESULT = "result";
    public static final String LABEL_SUSPENDED = "suspended";
    public static final String PROTOCOL_TYPE_GRPC = "grpc";

    private static LongHistogram rpcLatency = new NopLongHistogram();

    public static LongCounter messagesInTotal = new NopLongCounter();
    public static LongCounter messagesOutTotal = new NopLongCounter();
    public static LongCounter throughputInTotal = new NopLongCounter();
    public static LongCounter throughputOutTotal = new NopLongCounter();

    public static LongHistogram messageSize = new NopLongHistogram();

    public static ObservableLongGauge producerConnection = new NopObservableLongGauge();
    public static ObservableLongGauge consumerConnection = new NopObservableLongGauge();

    private static Supplier<AttributesBuilder> attributesBuilderSupplier;

    public static final List<String> SYSTEM_GROUP_PREFIX_LIST = new ArrayList<>() {
        {
            add(MixAll.CID_RMQ_SYS_PREFIX.toLowerCase());
        }
    };

    private final ExtendMessagingProcessor messagingProcessor;

    public ProxyMetricsManager(ExtendMessagingProcessor messagingProcessor) {
        this.messagingProcessor = messagingProcessor;
    }

    public static AttributesBuilder newAttributesBuilder() {
        if (attributesBuilderSupplier == null) {
            return Attributes.builder();
        }
        return attributesBuilderSupplier.get();
    }

    public static boolean isRetryOrDlqTopic(String topic) {
        if (StringUtils.isBlank(topic)) {
            return false;
        }
        return topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX) || topic.startsWith(MixAll.DLQ_GROUP_TOPIC_PREFIX);
    }

    public static boolean isSystemGroup(String group) {
        if (StringUtils.isBlank(group)) {
            return false;
        }
        String groupInLowerCase = group.toLowerCase();
        for (String prefix : SYSTEM_GROUP_PREFIX_LIST) {
            if (groupInLowerCase.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isSystem(String topic, String group) {
        return TopicValidator.isSystemTopic(topic) || isSystemGroup(group);
    }

    @Override
    public void initAttributesBuilder(Supplier<AttributesBuilder> attributesBuilderSupplier) {
        ProxyMetricsManager.attributesBuilderSupplier = attributesBuilderSupplier;
    }

    @Override
    public void initStaticMetrics(Meter meter) {
        rpcLatency = meter.histogramBuilder(HISTOGRAM_RPC_LATENCY)
            .setDescription("Rpc latency")
            .setUnit("milliseconds")
            .ofLongs()
            .build();

        messagesInTotal = meter.counterBuilder(COUNTER_MESSAGES_IN_TOTAL)
            .setDescription("Total number of incoming messages")
            .build();

        messagesOutTotal = meter.counterBuilder(COUNTER_MESSAGES_OUT_TOTAL)
            .setDescription("Total number of outgoing messages")
            .build();

        throughputInTotal = meter.counterBuilder(COUNTER_THROUGHPUT_IN_TOTAL)
            .setDescription("Total traffic of incoming messages")
            .build();

        throughputOutTotal = meter.counterBuilder(COUNTER_THROUGHPUT_OUT_TOTAL)
            .setDescription("Total traffic of outgoing messages")
            .build();

        messageSize = meter.histogramBuilder(HISTOGRAM_MESSAGE_SIZE)
            .setDescription("Incoming messages size")
            .ofLongs()
            .build();
    }

    @Override
    public void initDynamicMetrics(Meter meter) {
        producerConnection = meter.gaugeBuilder(GAUGE_PRODUCER_CONNECTIONS)
            .setDescription("Producer connections")
            .ofLongs()
            .buildWithCallback(measurement -> {
                Map<ProducerAttr, Integer> metricsMap = new HashMap<>();
                messagingProcessor.producerManager()
                    .getGroupChannelTable()
                    .values()
                    .stream()
                    .flatMap(map -> map.values().stream())
                    .forEach(info -> {
                        ProducerAttr attr = new ProducerAttr(info.getLanguage(), info.getVersion());
                        Integer count = metricsMap.computeIfAbsent(attr, k -> 0);
                        metricsMap.put(attr, count + 1);
                    });
                metricsMap.forEach((attr, count) -> {
                    Attributes attributes = newAttributesBuilder()
                        .put(LABEL_LANGUAGE, attr.language.name().toLowerCase())
                        .put(LABEL_VERSION, MQVersion.getVersionDesc(attr.version).toLowerCase())
                        .put(LABEL_PROTOCOL_TYPE, PROTOCOL_TYPE_GRPC)
                        .build();
                    measurement.record(count, attributes);
                });
            });

        consumerConnection = meter.gaugeBuilder(GAUGE_CONSUMER_CONNECTIONS)
            .setDescription("Consumer connections")
            .ofLongs()
            .buildWithCallback(measurement -> {
                Map<ConsumerAttr, Integer> metricsMap = new HashMap<>();
                ConsumerManager consumerManager = messagingProcessor.consumerManager();
                consumerManager.getConsumerTable()
                    .forEach((group, groupInfo) -> {
                        if (groupInfo != null) {
                            groupInfo.getChannelInfoTable().values().forEach(info -> {
                                ConsumerAttr attr = new ConsumerAttr(group, info.getLanguage(), info.getVersion(), groupInfo.getConsumeType());
                                Integer count = metricsMap.computeIfAbsent(attr, k -> 0);
                                metricsMap.put(attr, count + 1);
                            });
                        }
                    });
                metricsMap.forEach((attr, count) -> {
                    Attributes attributes = newAttributesBuilder()
                        .put(LABEL_CONSUMER_GROUP, attr.group)
                        .put(LABEL_LANGUAGE, attr.language.name().toLowerCase())
                        .put(LABEL_VERSION, MQVersion.getVersionDesc(attr.version).toLowerCase())
                        .put(LABEL_CONSUME_MODE, attr.consumeMode.getTypeCN().toLowerCase())
                        .put(LABEL_PROTOCOL_TYPE, PROTOCOL_TYPE_GRPC)
                        .put(LABEL_IS_SYSTEM, isSystemGroup(attr.group))
                        .build();
                    measurement.record(count, attributes);
                });
            });
    }

    public static List<Pair<InstrumentSelector, View>> getMetricsView() {
        ArrayList<Pair<InstrumentSelector, View>> metricsViewList = new ArrayList<>();
        // message size buckets, 1k, 4k, 512k, 1M, 2M, 4M
        List<Double> messageSizeBuckets = Arrays.asList(
            1d * 1024, //1KB
            4d * 1024, //4KB
            512d * 1024, //512KB
            1d * 1024 * 1024, //1MB
            2d * 1024 * 1024, //2MB
            4d * 1024 * 1024 //4MB
        );
        InstrumentSelector messageSizeSelector = InstrumentSelector.builder()
            .setType(InstrumentType.HISTOGRAM)
            .setName(HISTOGRAM_MESSAGE_SIZE)
            .build();
        ViewBuilder messageSizeViewBuilder = View.builder()
            .setAggregation(Aggregation.explicitBucketHistogram(messageSizeBuckets));
        metricsViewList.add(Pair.of(messageSizeSelector, messageSizeViewBuilder.build()));

        List<Double> rpcCostTimeBuckets = Arrays.asList(
            (double) Duration.ofNanos(100).toNanos(),
            (double) Duration.ofNanos(1000).toNanos(),
            (double) Duration.ofNanos(10_000).toNanos(),
            (double) Duration.ofNanos(100_000).toNanos(),
            (double) Duration.ofMillis(1).toNanos(),
            (double) Duration.ofMillis(2).toNanos(),
            (double) Duration.ofMillis(3).toNanos(),
            (double) Duration.ofMillis(5).toNanos(),
            (double) Duration.ofMillis(7).toNanos(),
            (double) Duration.ofMillis(10).toNanos(),
            (double) Duration.ofMillis(15).toNanos(),
            (double) Duration.ofMillis(30).toNanos(),
            (double) Duration.ofMillis(50).toNanos(),
            (double) Duration.ofMillis(100).toNanos(),
            (double) Duration.ofSeconds(1).toNanos(),
            (double) Duration.ofSeconds(2).toNanos(),
            (double) Duration.ofSeconds(3).toNanos()
        );
        InstrumentSelector selector = InstrumentSelector.builder()
            .setType(InstrumentType.HISTOGRAM)
            .setName(HISTOGRAM_RPC_LATENCY)
            .build();
        View view = View.builder()
            .setAggregation(Aggregation.explicitBucketHistogram(rpcCostTimeBuckets))
            .build();
        metricsViewList.add(Pair.of(selector, view));

        return metricsViewList;
    }

    public static void recordRpcLatency(String protocolType, String action, String result, long costTimeNanos) {
        recordRpcLatency(protocolType, action, result, costTimeNanos, false);
    }

    public static void recordRpcLatency(String protocolType, String action, String result, long costTimeNanos,
        boolean suspended) {
        AttributesBuilder attributesBuilder = newAttributesBuilder()
            .put(LABEL_PROTOCOL_TYPE, protocolType)
            .put(LABEL_ACTION, action)
            .put(LABEL_RESULT, result)
            .put(LABEL_SUSPENDED, suspended);
        rpcLatency.record(costTimeNanos, attributesBuilder.build());
    }

    public static void recordIncomingMessages(String topic, TopicMessageType messageType, int count, long size) {
        Attributes attributes = newAttributesBuilder()
            .put(LABEL_TOPIC, topic)
            .put(LABEL_MESSAGE_TYPE, messageType.getMetricsValue())
            .put(LABEL_IS_SYSTEM, TopicValidator.isSystemTopic(topic))
            .build();
        messagesInTotal.add(count, attributes);
        throughputInTotal.add(size, attributes);
        messageSize.record(size / count, attributes);
    }

    public static void recordOutgoingMessages(String topic, String consumerGroup, int count, long size,
        boolean isRetry) {
        Attributes attributes = newAttributesBuilder()
            .put(LABEL_TOPIC, topic)
            .put(LABEL_CONSUMER_GROUP, consumerGroup)
            .put(LABEL_IS_SYSTEM, TopicValidator.isSystemTopic(topic) || MixAll.isSysConsumerGroup(consumerGroup))
            .put(LABEL_IS_RETRY, isRetry)
            .build();
        messagesOutTotal.add(count, attributes);
        throughputOutTotal.add(size, attributes);
    }
}
