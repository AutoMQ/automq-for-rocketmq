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

package com.automq.rocketmq.proxy.grpc;

import apache.rocketmq.proxy.v1.ConsumerClientConnection;
import apache.rocketmq.proxy.v1.ConsumerClientConnectionRequest;
import apache.rocketmq.proxy.v1.ProducerClientConnection;
import apache.rocketmq.proxy.v1.ProducerClientConnectionRequest;
import apache.rocketmq.proxy.v1.QueueStats;
import apache.rocketmq.proxy.v1.ResetConsumeOffsetByTimestampRequest;
import apache.rocketmq.proxy.v1.ResetConsumeOffsetRequest;
import apache.rocketmq.proxy.v1.Status;
import apache.rocketmq.proxy.v1.TopicStatsRequest;
import com.automq.rocketmq.common.model.generated.FlatMessage;
import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface ProxyClient extends Closeable {
    CompletableFuture<Void> resetConsumeOffset(String target, ResetConsumeOffsetRequest request);

    CompletableFuture<Void> resetConsumeOffsetByTimestamp(String target, ResetConsumeOffsetByTimestampRequest request);

    CompletableFuture<List<QueueStats>> getTopicStats(String target, TopicStatsRequest request);

    CompletableFuture<List<ProducerClientConnection>> producerClientConnection(String target,
        ProducerClientConnectionRequest request);

    CompletableFuture<List<ConsumerClientConnection>> consumerClientConnection(String target,
        ConsumerClientConnectionRequest request);

    CompletableFuture<Status> relayMessage(String target, FlatMessage message);
}
