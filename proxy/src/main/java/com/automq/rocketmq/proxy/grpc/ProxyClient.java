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
