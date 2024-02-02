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

package com.automq.rocketmq.cli;

import apache.rocketmq.controller.v1.Cluster;
import apache.rocketmq.controller.v1.ClusterSummary;
import apache.rocketmq.controller.v1.MessageQueueAssignment;
import apache.rocketmq.controller.v1.Node;
import apache.rocketmq.controller.v1.OngoingMessageQueueReassignment;
import apache.rocketmq.controller.v1.Range;
import apache.rocketmq.controller.v1.StreamMetadata;
import apache.rocketmq.controller.v1.Topic;
import com.google.protobuf.Timestamp;
import de.vandermeer.asciitable.AT_Cell;
import de.vandermeer.asciitable.AT_Row;
import de.vandermeer.asciitable.AsciiTable;
import de.vandermeer.asciitable.CWC_LongestLine;
import de.vandermeer.skb.interfaces.transformers.textformat.TextAlignment;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ConsoleHelper {

    private static Date toDate(Timestamp timestamp) {
        long millis = TimeUnit.SECONDS.toMillis(timestamp.getSeconds()) + TimeUnit.NANOSECONDS.toMillis(timestamp.getNanos());
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(millis);
        return calendar.getTime();
    }

    public static void alignCentral(AT_Row row) {
        for (AT_Cell cell : row.getCells()) {
            cell.getContext().setTextAlignment(TextAlignment.CENTER);
        }
    }

    public static void printCluster(Cluster cluster) {
        if (null == cluster) {
            return;
        }

        CWC_LongestLine cwc = new CWC_LongestLine();

        // Cluster Summary
        AsciiTable summary = new AsciiTable();
        summary.addRule();
        AT_Row row = summary.addRow(null, null, null, null, "CLUSTER SUMMARY");
        alignCentral(row);
        summary.addRule();
        row = summary.addRow("NODE QUANTITY", "TOPIC QUANTITY", "QUEUE QUANTITY", "STREAM QUANTITY",
            "GROUP QUANTITY");
        alignCentral(row);
        ClusterSummary cs = cluster.getSummary();
        row = summary.addRow(cs.getNodeQuantity(), cs.getTopicQuantity(), cs.getQueueQuantity(),
            cs.getStreamQuantity(), cs.getGroupQuantity());
        alignCentral(row);
        summary.addRule();
        summary.getRenderer().setCWC(cwc);
        String render = summary.render();
        System.out.println(render);

        // Nodes List
        AsciiTable nodeTable = new AsciiTable();
        nodeTable.addRule();
        row = nodeTable.addRow("NODE ID", "NODE NAME", "TOPIC QUANTITY", "QUEUE QUANTITY",
            "STREAM QUANTITY", "LAST HEARTBEAT", "ROLE", "EPOCH", "EXPIRATION", "ADDRESS");

        alignCentral(row);

        for (Node node : cluster.getNodesList()) {
            nodeTable.addRule();
            boolean isLeader = node.getId() == cluster.getLease().getNodeId();
            row = nodeTable.addRow(node.getId(), node.getName(), node.getTopicNum(), node.getQueueNum(), node.getStreamNum(),
                toDate(node.getLastHeartbeat()), isLeader ? "Leader" : "Worker", isLeader ? cluster.getLease().getEpoch() : "",
                isLeader ? toDate(cluster.getLease().getExpirationTimestamp()) : "", node.getAddress());
            alignCentral(row);
        }
        nodeTable.addRule();

        nodeTable.getRenderer().setCWC(cwc);
        render = nodeTable.render();
        System.out.println(render);
    }

    public static void printTable(Topic topic) {
        AsciiTable topicTable = new AsciiTable();
        topicTable.addRule();
        topicTable.addRow("TOPIC ID", "TOPIC NAME");
        topicTable.addRule();
        topicTable.addRow(topic.getTopicId(), topic.getName());
        topicTable.addRule();
        String render = topicTable.render();
        System.out.println(render);

        AsciiTable assignmentTable = new AsciiTable();
        assignmentTable.addRule();
        AT_Row row = assignmentTable.addRow(null, "ASSIGNMENT");
        row.getCells().get(1).getContext().setTextAlignment(TextAlignment.CENTER);
        assignmentTable.addRule();
        assignmentTable.addRow("QUEUE ID", "NODE ID");
        assignmentTable.addRule();
        for (MessageQueueAssignment assignment : topic.getAssignmentsList()) {
            assignmentTable.addRow(assignment.getQueue().getQueueId(), assignment.getNodeId());
            assignmentTable.addRule();
        }
        render = assignmentTable.render();
        System.out.println(render);

        List<OngoingMessageQueueReassignment> ongoing = topic.getReassignmentsList();
        if (!ongoing.isEmpty()) {
            AsciiTable reassignmentTable = new AsciiTable();
            assignmentTable.addRule();
            row = assignmentTable.addRow(null, "ON-GOING REASSIGNMENT");
            row.getCells().get(1).getContext().setTextAlignment(TextAlignment.CENTER);
            reassignmentTable.addRule();
            reassignmentTable.addRow("SRC NODE ID", "DST NODE ID", "QUEUE ID");
            reassignmentTable.addRule();
            for (OngoingMessageQueueReassignment reassignment : ongoing) {
                reassignmentTable.addRow(reassignment.getSrcNodeId(), reassignment.getDstNodeId(),
                    reassignment.getQueue().getQueueId());
                reassignmentTable.addRule();
            }
        }
    }

    public static void printStream(StreamMetadata stream, List<Range> list) {
        AsciiTable streamTable = new AsciiTable();
        streamTable.addRule();
        AT_Row row = streamTable.addRow(null, null, null, null, "STREAM");
        alignCentral(row);
        streamTable.addRule();
        row = streamTable.addRow("STREAM ID", "EPOCH", "STATE", "START OFFSET", "END OFFSET");
        alignCentral(row);
        streamTable.addRule();
        row = streamTable.addRow(stream.getStreamId(), stream.getEpoch(), stream.getState(), stream.getStartOffset(), stream.getEndOffset());
        alignCentral(row);
        streamTable.addRule();
        String render = streamTable.render();
        System.out.println(render);

        if (list.isEmpty()) {
            AsciiTable rangeTable = new AsciiTable();
            rangeTable.addRule();
            row = rangeTable.addRow(null, null, null, null, "RANGES");
            alignCentral(row);
            rangeTable.addRule();
            row = rangeTable.addRow("RANGE ID", "EPOCH", "START OFFSET", "END OFFSET", "NODE");
            alignCentral(row);
            rangeTable.addRule();

            list.forEach(r -> {
                AT_Row rowOfRange = rangeTable.addRow(r.getRangeId(), r.getEpoch(), r.getStartOffset(), r.getEndOffset(), r.getBrokerId());
                alignCentral(rowOfRange);
                rangeTable.addRule();
            });
        }
    }
}
