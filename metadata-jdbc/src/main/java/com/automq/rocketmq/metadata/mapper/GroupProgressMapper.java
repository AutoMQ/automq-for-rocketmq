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

import com.automq.rocketmq.metadata.dao.GroupProgress;
import java.util.List;
import org.apache.ibatis.annotations.Param;

public interface GroupProgressMapper {

    int createOrUpdate(GroupProgress progress);

    List<GroupProgress> list(@Param("groupId") Long groupId, @Param("topicId") Long topicId);

    int delete(@Param("groupId")Long groupId, @Param("topicId") Long topicId);

    GroupProgress get(@Param("groupId")Long groupId, @Param("topicId") Long topicId, @Param("queueId") Integer queueId);
}
