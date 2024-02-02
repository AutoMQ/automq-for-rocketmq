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
import apache.rocketmq.controller.v1.MessageQueue;
import apache.rocketmq.controller.v1.MessageQueueAssignment;
import apache.rocketmq.controller.v1.Node;
import apache.rocketmq.controller.v1.Topic;
import com.automq.rocketmq.cli.CliClientConfig;
import com.automq.rocketmq.cli.MQAdmin;
import com.automq.rocketmq.controller.ControllerClient;
import com.automq.rocketmq.controller.client.GrpcControllerClient;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import picocli.CommandLine;

@CommandLine.Command(name = "reassignTopic", mixinStandardHelpOptions = true, showDefaultValues = true)
public class ReassignTopic implements Callable<Void> {
    @CommandLine.ParentCommand
    MQAdmin mqAdmin;

    @CommandLine.Option(names = {"-t", "--topicName"}, description = "Topic name", required = true)
    String topicName;

    @CommandLine.Option(names = {"-q", "--queueId"}, description = "Queue id, -1 means all queues of given topic", required = true, defaultValue = "-1")
    int queueId;

    @CommandLine.Option(names = {"-n", "--nodeId"}, description = "Node ID", required = true)
    int nodeId;

    @Override
    public Void call() throws Exception {
        try (ControllerClient client = new GrpcControllerClient(new CliClientConfig())) {
            Topic topic = client.describeTopic(mqAdmin.getEndpoint(), null, topicName).join();
            if (null == topic) {
                System.err.printf("Topic '%s' is not found%n%n", topicName);
                return null;
            }

            Cluster cluster = client.describeCluster(mqAdmin.getEndpoint(), DescribeClusterRequest.newBuilder().build()).join();
            Optional<Node> optionalNode = cluster.getNodesList().stream().filter(node -> node.getId() == nodeId).findFirst();
            if (optionalNode.isEmpty()) {
                System.err.printf("Node %d is not found%n%n", nodeId);
                return null;
            }

            if (queueId == -1) {
                List<MessageQueueAssignment> needToReassignQueueList = topic.getAssignmentsList().stream().filter(assignment -> assignment.getNodeId() != nodeId).toList();
                for (MessageQueueAssignment assignment : needToReassignQueueList) {
                    MessageQueue queue = assignment.getQueue();
                    client.reassignMessageQueue(mqAdmin.getEndpoint(), queue.getTopicId(), queue.getQueueId(), nodeId).join();
                    System.out.printf("Reassign queue %d of topic %s from node %d to node %d%n", queue.getQueueId(), topicName, assignment.getNodeId(), nodeId);
                }
            } else {
                Optional<MessageQueueAssignment> optionalAssignment = topic.getAssignmentsList().stream().filter(assignment -> assignment.getQueue().getQueueId() == queueId).findFirst();
                if (optionalAssignment.isEmpty()) {
                    System.err.printf("Queue %d of topic %s is not found%n%n", queueId, topicName);
                    return null;
                }
                MessageQueueAssignment assignment = optionalAssignment.get();
                if (assignment.getNodeId() == nodeId) {
                    System.out.printf("Queue %d of topic %s is already assigned to node %d%n", queueId, topicName, nodeId);
                    return null;
                }
                MessageQueue queue = assignment.getQueue();
                client.reassignMessageQueue(mqAdmin.getEndpoint(), queue.getTopicId(), queue.getQueueId(), nodeId).join();
                System.out.printf("Reassign queue %d of topic %s from node %d to node %d%n", queue.getQueueId(), topicName, assignment.getNodeId(), nodeId);
            }
        }
        return null;
    }
}
