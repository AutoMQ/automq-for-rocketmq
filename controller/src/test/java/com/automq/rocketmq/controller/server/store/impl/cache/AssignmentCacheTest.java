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