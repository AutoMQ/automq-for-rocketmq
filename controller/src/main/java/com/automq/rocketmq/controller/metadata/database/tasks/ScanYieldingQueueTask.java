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

package com.automq.rocketmq.controller.metadata.database.tasks;

import apache.rocketmq.controller.v1.AssignmentStatus;
import com.automq.rocketmq.common.StoreHandle;
import com.automq.rocketmq.controller.metadata.MetadataStore;
import com.automq.rocketmq.controller.metadata.database.dao.QueueAssignment;
import com.automq.rocketmq.controller.metadata.database.mapper.QueueAssignmentMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.StreamMapper;
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
 *
 * For each record found during the scan, the task will set up a state machine {@link NextState} and drive the state
 * machine to complete.
 *
 *
 */
public class ScanYieldingQueueTask extends ScanTask {

    private static enum NextState {
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
            try (SqlSession session = metadataStore.openSession()) {
                StreamMapper streamMapper = session.getMapper(StreamMapper.class);
                StoreHandle storeHandle = metadataStore.getStoreHandle();
                switch (next) {
                    case STORE_CLOSE -> closeQueue(streamMapper, storeHandle,
                        assignment.getTopicId(), assignment.getQueueId());

                    case NOTIFY_LEADER -> ScanYieldingQueueTask.this.metadataStore
                        .onQueueClosed(assignment.getTopicId(), assignment.getQueueId())
                        .whenComplete((res, e) -> {
                            if (null != e) {
                                next = NextState.COMPLETED;
                                doNext();
                            }
                        });
                    case COMPLETED ->
                        ScanYieldingQueueTask.this.doComplete(assignment.getTopicId(), assignment.getQueueId());
                }
                session.commit();
            }
        }

        private void closeQueue(StreamMapper mapper, StoreHandle handle, long topicId, int queueId) {
            long epoch = mapper.queueEpoch(topicId, queueId);
            handle.onTopicQueueClose(topicId, queueId, epoch)
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
    public void run() {
        LOGGER.info("Start to scan yielding queues");

        try (SqlSession session = metadataStore.openSession()) {
            QueueAssignmentMapper assignmentMapper = session.getMapper(QueueAssignmentMapper.class);
            List<QueueAssignment> assignments = assignmentMapper.list(null, metadataStore.config().nodeId(),
                null, AssignmentStatus.ASSIGNMENT_STATUS_YIELDING, this.lastScanTime);

            StoreHandle storeHandle = metadataStore.getStoreHandle();

            if (null != storeHandle && !assignments.isEmpty()) {
                for (QueueAssignment assignment : assignments) {
                    this.assignments.putIfAbsent(new ImmutablePair<>(assignment.getTopicId(), assignment.getQueueId()),
                        new QueueAssignmentStateMachine(assignment));
                }
            }

            for (Map.Entry<Pair<Long, Integer>, QueueAssignmentStateMachine> entry : this.assignments.entrySet()) {
                entry.getValue().doNext();
            }
        } catch (Throwable e) {
            LOGGER.error("Unexpected error raised", e);
        }

        LOGGER.info("Scan-yielding-queue completed");
    }
}
