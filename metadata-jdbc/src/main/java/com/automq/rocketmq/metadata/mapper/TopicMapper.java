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

import apache.rocketmq.controller.v1.TopicStatus;
import com.automq.rocketmq.metadata.dao.Topic;
import java.util.Date;
import java.util.List;
import org.apache.ibatis.annotations.Param;

public interface TopicMapper {

    int create(Topic topic);

    Topic get(@Param("id") Long id, @Param("name") String name);

    int updateStatusById(@Param("id") long id, @Param("status") TopicStatus status);

    List<Topic> list(@Param("status") TopicStatus status, @Param("updateTime") Date updateTime);

    int delete(Long id);

    int update(Topic topic);

    int recycle(@Param("status") TopicStatus status, @Param("updateTime") Date updateTime);

}
