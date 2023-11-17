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

package com.automq.rocketmq.controller.server.store.impl;

import apache.rocketmq.controller.v1.AcceptTypes;
import apache.rocketmq.controller.v1.AssignmentStatus;
import apache.rocketmq.controller.v1.MessageType;
import apache.rocketmq.controller.v1.TopicStatus;
import com.automq.rocketmq.metadata.dao.QueueAssignment;
import com.automq.rocketmq.metadata.dao.Topic;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Level;

public class TopicManagerBenchmark {

    @State(Scope.Benchmark)
    public static class TopicManagerBenchmarkPlan {

        public TopicManager topicManager;

        @Setup(Level.Trial)
        public void setUp() throws InvalidProtocolBufferException {
            topicManager = new TopicManager(null);
            Topic topic = new Topic();
            topic.setName("T1");
            topic.setId(1);
            topic.setQueueNum(1);
            topic.setRetentionHours(3);

            AcceptTypes.Builder acceptTypes = AcceptTypes.newBuilder()
                .addTypes(MessageType.NORMAL)
                .addTypes(MessageType.DELAY)
                .addTypes(MessageType.TRANSACTION);
            String types = JsonFormat.printer().print(acceptTypes);
            topic.setAcceptMessageTypes(types);
            topic.setStatus(TopicStatus.TOPIC_STATUS_ACTIVE);

            topicManager.topicCache.apply(List.of(topic));

            QueueAssignment assignment = new QueueAssignment();
            assignment.setStatus(AssignmentStatus.ASSIGNMENT_STATUS_ASSIGNED);
            assignment.setTopicId(1);
            assignment.setDstNodeId(1);
            assignment.setSrcNodeId(2);
            assignment.setQueueId(1);
            topicManager.assignmentCache.apply(List.of(assignment));
        }
    }

    @Benchmark
    @Fork(value = 1, warmups = 1)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void benchmark(TopicManagerBenchmarkPlan plan) {
        plan.topicManager.describeTopic(null, "T1");
    }
}
