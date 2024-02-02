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
import com.automq.rocketmq.common.api.DataStore;
import com.automq.rocketmq.common.exception.ControllerException;
import com.automq.rocketmq.controller.MetadataStore;
import com.automq.rocketmq.metadata.dao.QueueAssignment;
import com.automq.rocketmq.metadata.mapper.QueueAssignmentMapper;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.ibatis.session.SqlSession;

/**
 * This task will scan database incrementally to figure out queues that has been re-assigned to other nodes by leader
 * controller.
 * <p>
 * For each record found during the scan, the task will set up a state machine {@link NextState} and drive the state
 * machine to complete.
 */
public class ScanYieldingQueueTask extends ScanTask {

    private enum NextState {
        /**
         * Next step is to close queue in the underlying store.
         */
        STORE_CLOSE,

        /**
         * Once store has completed closing of the given queue/streams, notify the leader controller to modify status
         * of the queue and its streams state, such that newly assigned node could open them for incoming traffic.
         */
        NOTIFY_LEADER,

        /**
         * Once the previous two steps are completed, task is safe to remove all resources involved.
         */
        COMPLETED
    }

    private class QueueAssignmentStateMachine {
        final QueueAssignment assignment;
        NextState next = NextState.STORE_CLOSE;

        public QueueAssignmentStateMachine(QueueAssignment assignment) {
            this.assignment = assignment;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            QueueAssignmentStateMachine machine = (QueueAssignmentStateMachine) o;
            return assignment.getTopicId() == machine.assignment.getTopicId() &&
                assignment.getQueueId() == machine.assignment.getQueueId();
        }

        @Override
        public int hashCode() {
            return Objects.hash(assignment.getTopicId(), assignment.getQueueId());
        }

        public void doNext() {
            DataStore dataStore = metadataStore.getDataStore();
            switch (next) {
                case STORE_CLOSE -> {
                    LOGGER.info("Invoke DataStore to close queue[topic-id={}, queue-id={}]", assignment.getTopicId(),
                        assignment.getQueueId());
                    closeQueue(dataStore, assignment.getTopicId(), assignment.getQueueId());
                }

                case NOTIFY_LEADER -> {
                    LOGGER.info("Notify controller leader that DataStore has already closed queue[topic-id={}, queue-id={}]",
                        assignment.getTopicId(), assignment.getQueueId());
                    ScanYieldingQueueTask.this.metadataStore
                        .onQueueClosed(assignment.getTopicId(), assignment.getQueueId())
                        .whenComplete((res, e) -> {
                            if (null != e) {
                                next = NextState.COMPLETED;
                                doNext();
                                LOGGER.info("Controller leader has completed assignment/stream status update for " +
                                    "topic-id={}, queue-id={}", assignment.getTopicId(), assignment.getQueueId());
                            }
                        });
                }

                case COMPLETED ->
                    ScanYieldingQueueTask.this.doComplete(assignment.getTopicId(), assignment.getQueueId());
            }
        }

        private void closeQueue(DataStore handle, long topicId, int queueId) {
            handle.closeQueue(topicId, queueId)
                .whenComplete((res, e) -> {
                    if (null != e) {
                        LOGGER.error("Failed to close queue[topic-id={}, queue-id={}]", topicId, queueId);
                        return;
                    }

                    next = NextState.NOTIFY_LEADER;
                    doNext();
                });
        }
    }

    private final ConcurrentMap<Pair<Long, Integer>, QueueAssignmentStateMachine> assignments;

    public ScanYieldingQueueTask(MetadataStore metadataStore) {
        super(metadataStore);
        assignments = new ConcurrentHashMap<>();
    }

    public void doComplete(long topicId, int queueId) {
        this.assignments.remove(new ImmutablePair<>(topicId, queueId));
    }

    @Override
    public void process() throws ControllerException {
        try (SqlSession session = metadataStore.openSession()) {
            QueueAssignmentMapper assignmentMapper = session.getMapper(QueueAssignmentMapper.class);
            List<QueueAssignment> assignments = assignmentMapper.list(null, metadataStore.config().nodeId(),
                null, AssignmentStatus.ASSIGNMENT_STATUS_YIELDING, this.lastScanTime);

            DataStore dataStore = metadataStore.getDataStore();

            if (null != dataStore && !assignments.isEmpty()) {
                for (QueueAssignment assignment : assignments) {
                    QueueAssignmentStateMachine prev = this.assignments.putIfAbsent(new ImmutablePair<>(assignment.getTopicId(),
                            assignment.getQueueId()),
                        new QueueAssignmentStateMachine(assignment));
                    if (null == prev) {
                        LOGGER.info("Node[node-id={}] starts to yield topic-id={} queue-id={} to node[node-id={}]",
                            assignment.getSrcNodeId(), assignment.getTopicId(), assignment.getQueueId(),
                            assignment.getDstNodeId());
                    }
                }
            }

            for (Map.Entry<Pair<Long, Integer>, QueueAssignmentStateMachine> entry : this.assignments.entrySet()) {
                entry.getValue().doNext();
            }
        }
    }
}
