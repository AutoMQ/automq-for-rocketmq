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

package com.automq.rocketmq.cli;

import apache.rocketmq.controller.v1.Cluster;
import apache.rocketmq.controller.v1.DescribeClusterRequest;
import apache.rocketmq.controller.v1.MessageQueue;
import apache.rocketmq.controller.v1.MessageQueueAssignment;
import apache.rocketmq.controller.v1.Node;
import apache.rocketmq.controller.v1.Topic;
import apache.rocketmq.proxy.v1.ResetConsumeOffsetRequest;
import com.automq.rocketmq.controller.client.GrpcControllerClient;
import com.automq.rocketmq.proxy.grpc.client.GrpcProxyClient;
import com.google.protobuf.TextFormat;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import picocli.CommandLine;

@CommandLine.Command(name = "resetConsumeOffset", description = "Reset consume offset", mixinStandardHelpOptions = true, showDefaultValues = true)
public class ResetConsumeOffset implements Callable<Void> {

    @CommandLine.Option(names = {"-t", "--topic"}, description = "Topic name", required = true)
    String topicName;

    @CommandLine.Option(names = {"-g", "--group"}, description = "Consumer group name", required = true)
    String consumerGroupName;

    @CommandLine.Option(names = {"-q", "--queueId"}, description = "Queue id, -1 means all queues of given topic", required = true, defaultValue = "-1")
    int queueId;
    @CommandLine.Option(names = {"-o", "--offset"}, description = "New consume offset", required = true)
    long newConsumeOffset;

    @CommandLine.ParentCommand
    MQAdmin mqAdmin;

    @Override
    public Void call() throws Exception {
        // TODO: support retrying when failed because of cluster's state change at the same time
        GrpcControllerClient controllerClient = new GrpcControllerClient(new CliClientConfig());
        GrpcProxyClient proxyClient = new GrpcProxyClient(new CliClientConfig());
        CompletableFuture<Cluster> clusterCf = controllerClient.describeCluster(mqAdmin.endpoint, DescribeClusterRequest.newBuilder().build());

        CompletableFuture<Topic> topicCf = controllerClient.describeTopic(mqAdmin.endpoint, null, topicName);
        clusterCf.thenCombine(topicCf, Pair::of)
            .thenComposeAsync(pair -> {
                Cluster cluster = pair.getLeft();
                Topic topic = pair.getRight();
                Map<Integer/*node id*/, Node> nodeMap = cluster.getNodesList().stream().collect(Collectors.toMap(node -> node.getId(), node -> node));
                if (queueId == -1) {
                    // reset all queues
                    CompletableFuture[] tqCfs = topic.getAssignmentsList().stream().map(assignment -> {
                        int nodeId = assignment.getNodeId();
                        MessageQueue queue = assignment.getQueue();
                        Node node = nodeMap.get(nodeId);
                        if (node == null) {
                            System.out.println("Node not found: " + nodeId);
                            return CompletableFuture.completedFuture(null);
                        }
                        String brokerAddr = node.getAddress();
                        ResetConsumeOffsetRequest resetConsumeOffsetRequest = ResetConsumeOffsetRequest.newBuilder()
                            .setTopic(topicName)
                            .setQueueId(queue.getQueueId())
                            .setGroup(consumerGroupName)
                            .setNewConsumeOffset(newConsumeOffset)
                            .build();
                        return proxyClient.resetConsumeOffset(brokerAddr, resetConsumeOffsetRequest).thenAccept(nil -> {
                            System.out.println("Reset consume offset success: brokerAddress: " + brokerAddr + " , " + TextFormat.shortDebugString(resetConsumeOffsetRequest));
                        });
                    }).toArray(CompletableFuture[]::new);
                    return CompletableFuture.allOf(tqCfs);
                }
                // reset single queue
                Optional<MessageQueueAssignment> queueAssignment =
                    topic.getAssignmentsList().stream().filter(assignment -> assignment.getQueue().getQueueId() == queueId).findAny();
                if (queueAssignment.isEmpty()) {
                    System.out.println("Queue not found: " + queueId);
                    return CompletableFuture.completedFuture(null);
                }
                int nodeId = queueAssignment.get().getNodeId();
                Node node = nodeMap.get(nodeId);
                if (node == null) {
                    System.out.println("Node not found: " + nodeId);
                    return CompletableFuture.completedFuture(null);
                }
                String brokerAddr = node.getAddress();
                ResetConsumeOffsetRequest resetConsumeOffsetRequest = ResetConsumeOffsetRequest.newBuilder()
                    .setTopic(topicName)
                    .setQueueId(queueId)
                    .setGroup(consumerGroupName)
                    .setNewConsumeOffset(newConsumeOffset)
                    .build();
                return proxyClient.resetConsumeOffset(brokerAddr, resetConsumeOffsetRequest).thenAccept(nil -> {
                    System.out.println("Reset consume offset success: brokerAddress: " + brokerAddr + " , " + TextFormat.shortDebugString(resetConsumeOffsetRequest));
                });
            }).join();
        return null;
    }
}
