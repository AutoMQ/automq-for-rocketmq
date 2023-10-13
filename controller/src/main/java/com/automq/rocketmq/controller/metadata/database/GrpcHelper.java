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

package com.automq.rocketmq.controller.metadata.database;

import apache.rocketmq.controller.v1.MessageQueue;
import apache.rocketmq.controller.v1.MessageQueueAssignment;
import apache.rocketmq.controller.v1.MessageType;
import apache.rocketmq.controller.v1.OngoingMessageQueueReassignment;
import apache.rocketmq.controller.v1.Topic;
import com.automq.rocketmq.controller.metadata.database.dao.QueueAssignment;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.util.Collection;
import java.util.List;

public class GrpcHelper {
    public static Topic buildTopic(Gson gson,
        com.automq.rocketmq.controller.metadata.database.dao.Topic topic,
        Collection<QueueAssignment> assignments) {
        apache.rocketmq.controller.v1.Topic.Builder topicBuilder = apache.rocketmq.controller.v1.Topic
            .newBuilder()
            .setTopicId(topic.getId())
            .setCount(topic.getQueueNum())
            .setName(topic.getName())
            .addAllAcceptMessageTypes(gson.fromJson(topic.getAcceptMessageTypes(), new TypeToken<List<MessageType>>() {
            }.getType()));

        if (null != assignments) {
            for (QueueAssignment assignment : assignments) {
                switch (assignment.getStatus()) {
                    case ASSIGNMENT_STATUS_DELETED -> {
                    }

                    case ASSIGNMENT_STATUS_ASSIGNED -> {
                        MessageQueueAssignment queueAssignment = MessageQueueAssignment.newBuilder()
                            .setQueue(MessageQueue.newBuilder()
                                .setTopicId(assignment.getTopicId())
                                .setQueueId(assignment.getQueueId()))
                            .setNodeId(assignment.getDstNodeId())
                            .build();
                        topicBuilder.addAssignments(queueAssignment);
                    }

                    case ASSIGNMENT_STATUS_YIELDING -> {
                        OngoingMessageQueueReassignment reassignment = OngoingMessageQueueReassignment.newBuilder()
                            .setQueue(MessageQueue.newBuilder()
                                .setTopicId(assignment.getTopicId())
                                .setQueueId(assignment.getQueueId())
                                .build())
                            .setSrcNodeId(assignment.getSrcNodeId())
                            .setDstNodeId(assignment.getDstNodeId())
                            .build();
                        topicBuilder.addReassignments(reassignment);
                    }
                }
            }
        }
        return topicBuilder.build();
    }
}
