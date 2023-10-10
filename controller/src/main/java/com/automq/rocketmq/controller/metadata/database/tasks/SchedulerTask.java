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
import com.automq.rocketmq.controller.metadata.MetadataStore;
import com.automq.rocketmq.controller.metadata.database.dao.QueueAssignment;
import com.automq.rocketmq.controller.metadata.database.mapper.QueueAssignmentMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.StreamMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.ibatis.session.SqlSession;

public class SchedulerTask extends ControllerTask {
    public SchedulerTask(MetadataStore metadataStore) {
        super(metadataStore);
    }

    @Override
    public void run() {
        LOGGER.debug("SchedulerTask starts");
        try {
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
                assignments.forEach(assignment -> {
                    if (!workload.containsKey(assignment.getDstNodeId())) {
                        workload.put(assignment.getDstNodeId(), new ArrayList<>());
                    }
                    workload.get(assignment.getDstNodeId()).add(assignment);
                });

                if (doSchedule(session, workload)) {
                    session.commit();
                }
            }
        } catch (Throwable e) {
            LOGGER.error("Unexpected exception raised", e);
        }
        LOGGER.debug("SchedulerTask completed");
    }

    private boolean doSchedule(SqlSession session, Map<Integer, List<QueueAssignment>> workload) {
        boolean changed = false;

        QueueAssignmentMapper assignmentMapper = session.getMapper(QueueAssignmentMapper.class);
        StreamMapper streamMapper = session.getMapper(StreamMapper.class);

        for (; ; ) {

            if (workload.isEmpty()) {
                break;
            }

            if (workload.size() == 1) {
                break;
            }

            int dst = -1;

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
                    min.add(assignment);

                    assignmentMapper.update(assignment);
                    int cnt = streamMapper.planMove(assignment.getTopicId(), assignment.getQueueId(), assignment.getSrcNodeId(),
                        assignment.getDstNodeId());
                    LOGGER.info("Moved Queue[topic-id={}, queue-id={}], containing {} streams, from Node[node-id={}] to Node[node-id={}]",
                        assignment.getTopicId(), assignment.getQueueId(), cnt, assignment.getSrcNodeId(), assignment.getDstNodeId());
                    changed = true;
                    reassign = true;
                    break;
                }
            }

            if (!reassign) {
                // One of the node is not working properly, let's wait till the cluster runs stable
                break;
            }
        }

        return changed;
    }
}
