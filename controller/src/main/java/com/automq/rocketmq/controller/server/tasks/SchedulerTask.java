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

package com.automq.rocketmq.controller.server.tasks;

import apache.rocketmq.controller.v1.AssignmentStatus;
import apache.rocketmq.controller.v1.StreamState;
import com.automq.rocketmq.common.exception.ControllerException;
import com.automq.rocketmq.controller.MetadataStore;
import com.automq.rocketmq.metadata.dao.QueueAssignment;
import com.automq.rocketmq.metadata.dao.StreamCriteria;
import com.automq.rocketmq.metadata.mapper.QueueAssignmentMapper;
import com.automq.rocketmq.metadata.mapper.StreamMapper;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ibatis.session.SqlSession;

public class SchedulerTask extends ControllerTask {
    public SchedulerTask(MetadataStore metadataStore) {
        super(metadataStore);
    }

    @Override
    public void process() throws ControllerException {
        if (!metadataStore.isLeader()) {
            LOGGER.debug("Node[node-id={}] is not leader. Leader address is {}", metadataStore.config().nodeId(),
                metadataStore.leaderAddress());
            return;
        }

        try (SqlSession session = metadataStore.openSession()) {
            // Lock lease with shared lock
            if (!metadataStore.maintainLeadershipWithSharedLock(session)) {
                LOGGER.info("Node[node-id={}] lost leadership", metadataStore.config().nodeId());
                return;
            }

            QueueAssignmentMapper assignmentMapper = session.getMapper(QueueAssignmentMapper.class);
            List<QueueAssignment> assignments = assignmentMapper
                .list(null, null, null, null, null)
                .stream()
                .filter(assignment -> assignment.getStatus() != AssignmentStatus.ASSIGNMENT_STATUS_DELETED)
                .toList();

            Map<Integer, List<QueueAssignment>> workload = new HashMap<>();
            // Assignment that is orphan or about to be orphan.
            Map<Integer, List<QueueAssignment>> orphan = new HashMap<>();
            metadataStore
                .allNodes()
                .values()
                .forEach(node -> {
                    if (node.isAlive(metadataStore.config()) && !node.isGoingAway()) {
                        workload.put(node.getNode().getId(), new ArrayList<>());
                    } else {
                        orphan.put(node.getNode().getId(), new ArrayList<>());
                    }
                });

            assignments
                .forEach(assignment -> {
                    if (workload.containsKey(assignment.getDstNodeId())) {
                        workload.get(assignment.getDstNodeId()).add(assignment);
                    }

                    if (orphan.containsKey(assignment.getDstNodeId())) {
                        orphan.get(assignment.getDstNodeId()).add(assignment);
                    }
                });

            if (doSchedule(session, workload, orphan)) {
                session.commit();
            }
        }
    }

    private int pick(Map<Integer, List<QueueAssignment>> workload) {
        int id = 0;
        int load = Integer.MAX_VALUE;
        for (Map.Entry<Integer, List<QueueAssignment>> entry : workload.entrySet()) {
            if (entry.getValue().size() <= load) {
                id = entry.getKey();
                load = entry.getValue().size();
            }
        }
        return id;
    }

    private boolean doSchedule(SqlSession session,
        Map<Integer, List<QueueAssignment>> workload,
        Map<Integer, List<QueueAssignment>> orphan) {
        AtomicBoolean changed = new AtomicBoolean(false);

        QueueAssignmentMapper assignmentMapper = session.getMapper(QueueAssignmentMapper.class);
        StreamMapper streamMapper = session.getMapper(StreamMapper.class);

        if (!orphan.isEmpty()) {
            if (workload.isEmpty()) {
                LOGGER.warn("No serving node is available");
                return false;
            }

            orphan.values().stream().flatMap(Collection::stream)
                .filter(assignment -> assignment.getStatus() == AssignmentStatus.ASSIGNMENT_STATUS_ASSIGNED)
                .forEach(assignment -> {
                    // Pick a destination node that serves the fewest queues and streams.
                    int dst = pick(workload);
                    assignment.setSrcNodeId(assignment.getDstNodeId());
                    assignment.setDstNodeId(dst);
                    assignment.setStatus(AssignmentStatus.ASSIGNMENT_STATUS_YIELDING);
                    assignmentMapper.update(assignment);
                    workload.get(dst).add(assignment);
                    LOGGER.info("Let Node[node-id={}] yield queue[topic-id={}, queue-id={}] to Node[node-id={}]",
                        assignment.getSrcNodeId(), assignment.getTopicId(), assignment.getQueueId(),
                        assignment.getDstNodeId());
                    moveStreams(changed, streamMapper, assignment);
                });
        }

        // Ensure workload among active nodes are balanced.
        for (; ; ) {

            if (workload.isEmpty()) {
                break;
            }

            if (workload.size() == 1) {
                break;
            }

            int dst = -1;

            // Find nodes that has the most and fewest queues assigned or yielded to
            List<QueueAssignment> max = null;
            List<QueueAssignment> min = null;
            for (Map.Entry<Integer, List<QueueAssignment>> entry : workload.entrySet()) {
                if (null == max || entry.getValue().size() > max.size()) {
                    max = entry.getValue();
                }

                if (null == min || entry.getValue().size() < min.size()) {
                    min = entry.getValue();
                    dst = entry.getKey();
                }
            }

            if (max.size() <= min.size() + metadataStore.config().workloadTolerance()) {
                LOGGER.debug("Workload are already balanced, delta: {}, tolerance: {}", max.size() - min.size(),
                    metadataStore.config().workloadTolerance());
                break;
            }

            boolean reassign = false;
            Iterator<QueueAssignment> itr = max.iterator();
            while (itr.hasNext()) {
                QueueAssignment assignment = itr.next();
                if (assignment.getStatus() == AssignmentStatus.ASSIGNMENT_STATUS_ASSIGNED) {
                    itr.remove();
                    assignment.setSrcNodeId(assignment.getDstNodeId());
                    assert dst != -1;
                    assignment.setDstNodeId(dst);
                    assignment.setStatus(AssignmentStatus.ASSIGNMENT_STATUS_YIELDING);
                    min.add(assignment);

                    assignmentMapper.update(assignment);
                    moveStreams(changed, streamMapper, assignment);
                    reassign = true;
                    break;
                }
            }

            if (!reassign) {
                // One of the node is not working properly, let's wait till the cluster stabilizes.
                break;
            }
        }

        return changed.get();
    }

    private void moveStreams(AtomicBoolean changed, StreamMapper streamMapper, QueueAssignment assignment) {
        List<StreamState> movableStates = List.of(StreamState.UNINITIALIZED, StreamState.OPEN, StreamState.CLOSED);
        for (StreamState state : movableStates) {
            StreamCriteria criteria = StreamCriteria.newBuilder()
                .withTopicId(assignment.getTopicId())
                .withQueueId(assignment.getQueueId())
                .withState(state)
                .build();
            int cnt = streamMapper.planMove(criteria, assignment.getSrcNodeId(), assignment.getDstNodeId(),
                StreamState.OPEN == state ? StreamState.CLOSING : state);
            LOGGER.info("Moved {} streams of with topic-id={}, queue-id={}, state={} from Node[node-id={}] to Node[node-id={}]",
                cnt, assignment.getTopicId(), assignment.getQueueId(), state, assignment.getSrcNodeId(),
                assignment.getDstNodeId());
            if (cnt > 0) {
                changed.set(true);
            }
        }
    }
}
