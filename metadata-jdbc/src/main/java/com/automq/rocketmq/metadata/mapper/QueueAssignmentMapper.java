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
