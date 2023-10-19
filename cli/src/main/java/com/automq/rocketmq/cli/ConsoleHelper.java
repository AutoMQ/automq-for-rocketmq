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

import apache.rocketmq.controller.v1.MessageQueueAssignment;
import apache.rocketmq.controller.v1.OngoingMessageQueueReassignment;
import apache.rocketmq.controller.v1.Topic;
import de.vandermeer.asciitable.AT_Row;
import de.vandermeer.asciitable.AsciiTable;
import de.vandermeer.skb.interfaces.transformers.textformat.TextAlignment;
import java.util.List;

public class ConsoleHelper {
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
        assignmentTable.addRow("NODE ID", "QUEUE ID");
        assignmentTable.addRule();
        for (MessageQueueAssignment assignment : topic.getAssignmentsList()) {
            assignmentTable.addRow(assignment.getNodeId(), assignment.getQueue().getQueueId());
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
                reassignmentTable.addRow(reassignment.getSrcNodeId(), reassignment.getDstNodeId(), reassignment.getQueue().getQueueId());
                reassignmentTable.addRule();
            }
        }
    }
}
