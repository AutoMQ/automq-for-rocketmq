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

package com.automq.rocketmq.controller.metadata.database.mapper;

import com.automq.rocketmq.controller.metadata.database.dao.AssignmentStatus;
import com.automq.rocketmq.controller.metadata.database.dao.StreamAffiliation;
import java.util.Date;
import java.util.List;
import org.apache.ibatis.annotations.Param;

public interface StreamAffiliationMapper {
    int create(StreamAffiliation streamAffiliation);

    List<StreamAffiliation> list(@Param("topicId") Long topicId,
        @Param("queueId") Integer queueId,
        @Param("groupId") Long groupId,
        @Param("updateTime") Date updateTime);

    int delete(@Param("topicId") Long topicId, @Param("queueId") Integer queueId);

    /**
     * Update Stream status according to optional topic-id and group-id
     *
     * @param topicId
     * @param queueId
     * @param groupId
     * @param status
     * @return number of rows affected
     */
    int update(@Param("topicId") Long topicId,
        @Param("queueId") Integer queueId,
        @Param("groupId") Long groupId,
        @Param("srcNodeId") Integer srcNodeId,
        @Param("dstNodeId") Integer dstNodeId,
        @Param("status") AssignmentStatus status);
}
