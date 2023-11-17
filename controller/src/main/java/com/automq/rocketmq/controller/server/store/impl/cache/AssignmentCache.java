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

package com.automq.rocketmq.controller.server.store.impl.cache;

import com.automq.rocketmq.metadata.dao.QueueAssignment;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class AssignmentCache {

    private final ConcurrentMap<Long, Map<Integer, QueueAssignment>> assignments;

    public AssignmentCache() {
        assignments = new ConcurrentHashMap<>();
    }

    public void apply(List<QueueAssignment> assignments) {
        if (null == assignments || assignments.isEmpty()) {
            return;
        }

        for (QueueAssignment assignment : assignments) {
            cacheItem(assignment);
        }
    }

    public List<QueueAssignment> byNode(int nodeId) {
        List<QueueAssignment> result = new ArrayList<>();
        for (Map.Entry<Long, Map<Integer, QueueAssignment>> entry : assignments.entrySet()) {
            for (Map.Entry<Integer, QueueAssignment> e : entry.getValue().entrySet()) {
                switch (e.getValue().getStatus()) {
                    case ASSIGNMENT_STATUS_YIELDING -> {
                        if (e.getValue().getSrcNodeId() == nodeId) {
                            result.add(e.getValue());
                        }
                    }
                    case ASSIGNMENT_STATUS_ASSIGNED -> {
                        if (e.getValue().getDstNodeId() == nodeId) {
                            result.add(e.getValue());
                        }
                    }
                }
            }
        }
        return result;
    }

    public Map<Integer, QueueAssignment> byTopicId(Long topicId) {
        return assignments.get(topicId);
    }

    private void cacheItem(QueueAssignment assignment) {
        if (!assignments.containsKey(assignment.getTopicId())) {
            assignments.putIfAbsent(assignment.getTopicId(), new HashMap<>());
        }

        // Copy-on-Write
        Map<Integer, QueueAssignment> clone = new HashMap<>(assignments.get(assignment.getTopicId()));
        switch (assignment.getStatus()) {
            case ASSIGNMENT_STATUS_DELETED -> {
                clone.remove(assignment.getQueueId());
            }
            case ASSIGNMENT_STATUS_ASSIGNED, ASSIGNMENT_STATUS_YIELDING -> {
                clone.put(assignment.getQueueId(), assignment);
            }
        }
        if (!clone.isEmpty()) {
            assignments.put(assignment.getTopicId(), clone);
        } else {
            assignments.remove(assignment.getTopicId());
        }
    }

    public int topicNumOfNode(int nodeId) {
        int count = 0;
        for (Map.Entry<Long, Map<Integer, QueueAssignment>> entry : assignments.entrySet()) {
            for (Map.Entry<Integer, QueueAssignment> e : entry.getValue().entrySet()) {
                if (e.getValue().getDstNodeId() == nodeId) {
                    count++;
                    break;
                }
            }
        }
        return count;
    }

    public int queueNumOfNode(int nodeId) {
        int count = 0;
        for (Map.Entry<Long, Map<Integer, QueueAssignment>> entry : assignments.entrySet()) {
            for (Map.Entry<Integer, QueueAssignment> e : entry.getValue().entrySet()) {
                if (e.getValue().getDstNodeId() == nodeId) {
                    count++;
                }
            }
        }
        return count;
    }

    public int queueQuantity() {
        return assignments.values().stream().map(Map::size).reduce(0, Integer::sum);
    }
}
