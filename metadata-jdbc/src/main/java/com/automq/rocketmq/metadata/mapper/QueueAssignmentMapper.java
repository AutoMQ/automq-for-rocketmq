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

package com.automq.rocketmq.metadata.mapper;

import apache.rocketmq.controller.v1.AssignmentStatus;
import com.automq.rocketmq.metadata.dao.QueueAssignment;
import java.util.Date;
import java.util.List;
import org.apache.ibatis.annotations.Param;

public interface QueueAssignmentMapper {

    int create(QueueAssignment assignment);

    QueueAssignment get(@Param("topicId") long topicId, @Param("queueId") int queueId);

    List<QueueAssignment> list(@Param("topicId") Long topicId,
        @Param("srcNodeId") Integer srcNodeId,
        @Param("dstNodeId") Integer dstNodeId,
        @Param("status") AssignmentStatus status,
        @Param("updateTime") Date updateTime);

    int update(QueueAssignment assignment);

    /**
     * This method is for test only!!!
     * @param topicId Optional topic-id to delete its queue assignments; If null, all assignments are deleted.
     * @return Number of rows affected
     */
    int delete(Long topicId);
}
