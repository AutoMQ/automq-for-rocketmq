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

package com.automq.rocketmq.cli.topic;

import apache.rocketmq.controller.v1.Cluster;
import apache.rocketmq.controller.v1.DescribeClusterRequest;
import apache.rocketmq.controller.v1.MessageQueueAssignment;
import apache.rocketmq.controller.v1.Node;
import apache.rocketmq.controller.v1.StreamRole;
import apache.rocketmq.controller.v1.Topic;
import apache.rocketmq.proxy.v1.QueueStats;
import apache.rocketmq.proxy.v1.StreamStats;
import apache.rocketmq.proxy.v1.TopicStatsRequest;
import com.automq.rocketmq.cli.CliClientConfig;
import com.automq.rocketmq.cli.MQAdmin;
import com.automq.rocketmq.controller.client.GrpcControllerClient;
import com.automq.rocketmq.proxy.grpc.client.GrpcProxyClient;
import de.vandermeer.asciitable.AT_Row;
import de.vandermeer.asciitable.AsciiTable;
import de.vandermeer.asciitable.CWC_LongestLine;
import de.vandermeer.skb.interfaces.transformers.textformat.TextAlignment;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import picocli.CommandLine;

@CommandLine.Command(name = "printTopicStats", mixinStandardHelpOptions = true, showDefaultValues = true)
public class PrintTopicStats implements Callable<Void> {
    @CommandLine.Option(names = {"-t", "--topic"}, description = "Topic name", required = true)
    String topicName;

    @CommandLine.Option(names = {"-g", "--group"}, description = "Consumer group name", defaultValue = "")
    String consumerGroupName;

    @CommandLine.Option(names = {"-q", "--queueId"}, description = "Queue id, -1 means all queues of given topic", required = true, defaultValue = "-1")
    int queueId;

    @CommandLine.ParentCommand
    MQAdmin mqAdmin;

    @Override
    public Void call() throws Exception {
        GrpcControllerClient controllerClient = new GrpcControllerClient(new CliClientConfig());
        GrpcProxyClient proxyClient = new GrpcProxyClient(new CliClientConfig());

        CompletableFuture<Cluster> clusterFuture = controllerClient.describeCluster(mqAdmin.getEndpoint(), DescribeClusterRequest.newBuilder().build());
        CompletableFuture<Topic> topicFuture = controllerClient.describeTopic(mqAdmin.getEndpoint(), null, topicName);

        clusterFuture.thenCombine(topicFuture, (cluster, topic) -> {
            if (topic == null) {
                System.out.println("Topic " + topicName + " does not exist");
                return null;
            }

            Map<Integer/*node id*/, Node> nodeMap = cluster.getNodesList().stream().collect(Collectors.toMap(node -> node.getId(), node -> node));

            long topicId;
            List<QueueStats> queueStatsList = new ArrayList<>();
            if (queueId == -1) {
                List<Integer> nodeList = topic.getAssignmentsList().stream().map(MessageQueueAssignment::getNodeId).distinct().toList();
                for (int nodeId : nodeList) {
                    String nodeAddress = nodeMap.get(nodeId).getAddress();
                    List<QueueStats> result = proxyClient.getTopicStats(nodeAddress, TopicStatsRequest.newBuilder().setTopic(topicName).setGroup(consumerGroupName).build()).join();
                    queueStatsList.addAll(result);
                }
            } else {
                Optional<MessageQueueAssignment> optional = topic.getAssignmentsList().stream().filter(assignment -> assignment.getQueue().getQueueId() == queueId).findFirst();
                if (optional.isEmpty()) {
                    throw new IllegalArgumentException("Queue " + queueId + " does not exist");
                }
                String nodeAddress = nodeMap.get(optional.get().getNodeId()).getAddress();
                List<QueueStats> result = proxyClient.getTopicStats(nodeAddress, TopicStatsRequest.newBuilder().setTopic(topicName).setGroup(consumerGroupName).build()).join();
                queueStatsList.addAll(result);
            }
            printTopicStats(topic, queueStatsList);
            return null;
        }).get();
        return null;
    }

    private void printTopicStats(Topic topic, List<QueueStats> queueStatsList) {
        System.out.println("Topic Id: " + topic.getTopicId());
        System.out.println("Topic Name: " + topic.getName());

        int streamCount = queueStatsList.stream().map(QueueStats::getStreamStatsCount).mapToInt(Integer::intValue).sum();
        if (streamCount == 0) {
            System.out.println("No opened queue found");
            return;
        }

        AsciiTable statsTable = new AsciiTable();
        statsTable.addRule();
        AT_Row row = statsTable.addRow("TOPIC", "QUEUE ID", "STREAM ID", "STREAM ROLE", "MIN OFFSET", "MAX OFFSET", "CONSUME OFFSET", "DELIVERING COUNT");
        centralize(row);
        statsTable.addRule();

        for (QueueStats queueStats : queueStatsList) {
            for (StreamStats streamStats : queueStats.getStreamStatsList()) {
                row = statsTable.addRow(topic.getName(), queueStats.getQueueId(), streamStats.getStreamId(), formatStreamRole(streamStats.getRole(), consumerGroupName), streamStats.getMinOffset(), streamStats.getMaxOffset(), streamStats.getConsumeOffset(), "-");
                centralize(row);
                statsTable.addRule();
            }
        }

        CWC_LongestLine cwc = new CWC_LongestLine();
        IntStream.range(0, row.getCells().size()).forEach((i) -> cwc.add(10, 0));
        statsTable.getRenderer().setCWC(cwc);

        String render = statsTable.render();
        System.out.println(render);
    }

    private void centralize(AT_Row row) {
        row.getCells().forEach((cell -> cell.getContext().setTextAlignment(TextAlignment.CENTER)));
    }

    private String formatStreamRole(StreamRole role, String consumerGroup) {
        return switch (role) {
            case STREAM_ROLE_DATA -> "DATA";
            case STREAM_ROLE_OPS -> "OPERATION";
            case STREAM_ROLE_SNAPSHOT -> "SNAPSHOT";
            case STREAM_ROLE_RETRY -> "RETRY for " + consumerGroup;
            default -> "UNKNOWN";
        };
    }
}
