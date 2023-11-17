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

import apache.rocketmq.controller.v1.AssignmentStatus;
import com.automq.rocketmq.metadata.dao.QueueAssignment;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class AssignmentCacheTest {

    @Test
    public void testApply() {
        AssignmentCache cache = new AssignmentCache();
        QueueAssignment assignment = new QueueAssignment();
        assignment.setStatus(AssignmentStatus.ASSIGNMENT_STATUS_ASSIGNED);
        assignment.setTopicId(1);
        assignment.setQueueId(2);
        assignment.setSrcNodeId(3);
        assignment.setDstNodeId(4);
        cache.apply(List.of(assignment));
        Map<Integer, QueueAssignment> assignments = cache.byTopicId(1L);
        Assertions.assertEquals(1, assignments.size());

        Assertions.assertEquals(0, cache.topicNumOfNode(1));
        Assertions.assertEquals(1, cache.topicNumOfNode(4));

        Assertions.assertEquals(1, cache.queueNumOfNode(4));
        Assertions.assertEquals(0, cache.queueNumOfNode(3));
    }

    @Test
    public void testByNode() {
        AssignmentCache cache = new AssignmentCache();
        QueueAssignment assignment = new QueueAssignment();
        assignment.setStatus(AssignmentStatus.ASSIGNMENT_STATUS_ASSIGNED);
        assignment.setTopicId(1);
        assignment.setQueueId(2);
        assignment.setSrcNodeId(3);
        assignment.setDstNodeId(4);
        cache.apply(List.of(assignment));

        Assertions.assertFalse(cache.byNode(4).isEmpty());
        Assertions.assertTrue(cache.byNode(3).isEmpty());
    }

}