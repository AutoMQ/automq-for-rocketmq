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

package com.automq.stream.s3.metrics.stats;

import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.S3StreamMetricsManager;
import com.automq.stream.s3.metrics.wrapper.CounterMetric;
import com.automq.stream.s3.metrics.wrapper.YammerHistogramMetric;
import com.automq.stream.s3.network.AsyncNetworkBandwidthLimiter;
import com.yammer.metrics.core.MetricName;

public class NetworkStats {
    private volatile static NetworkStats instance = null;

    private final CounterMetric networkInboundUsageStats = S3StreamMetricsManager.buildNetworkInboundUsageMetric();
    private final CounterMetric networkOutboundUsageStats = S3StreamMetricsManager.buildNetworkOutboundUsageMetric();
    private final YammerHistogramMetric networkInboundLimiterQueueTimeStats = S3StreamMetricsManager.buildNetworkInboundLimiterQueueTimeMetric(
        new MetricName(NetworkStats.class, "NetworkInboundLimiterQueueTime"), MetricsLevel.INFO);
    private final YammerHistogramMetric networkOutboundLimiterQueueTimeStats = S3StreamMetricsManager.buildNetworkOutboundLimiterQueueTimeMetric(
        new MetricName(NetworkStats.class, "NetworkOutboundLimiterQueueTime"), MetricsLevel.INFO);

    private NetworkStats() {
    }

    public static NetworkStats getInstance() {
        if (instance == null) {
            synchronized (NetworkStats.class) {
                if (instance == null) {
                    instance = new NetworkStats();
                }
            }
        }
        return instance;
    }

    public CounterMetric networkUsageStats(AsyncNetworkBandwidthLimiter.Type type) {
        return type == AsyncNetworkBandwidthLimiter.Type.INBOUND ? networkInboundUsageStats : networkOutboundUsageStats;
    }

    public YammerHistogramMetric networkLimiterQueueTimeStats(AsyncNetworkBandwidthLimiter.Type type) {
        return type == AsyncNetworkBandwidthLimiter.Type.INBOUND ? networkInboundLimiterQueueTimeStats : networkOutboundLimiterQueueTimeStats;
    }
}
