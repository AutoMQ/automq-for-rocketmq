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
