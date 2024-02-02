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

import apache.rocketmq.controller.v1.GroupStatus;
import com.automq.rocketmq.metadata.dao.Group;
import com.automq.rocketmq.metadata.dao.GroupCriteria;
import java.util.Date;
import java.util.List;
import org.apache.ibatis.annotations.Param;

public interface GroupMapper {

    int create(Group group);

    List<Group> byCriteria(GroupCriteria criteria);

    int update(Group group);

    /**
     * Delete consumer group records from table.
     * <strong>Warning</strong>
     * If <code>id</code> is null, all records will be deleted.
     *
     * @param id Optional group-id to delete
     * @return Number of rows affected.
     */
    int delete(@Param("id") Long id);

    int recycle(@Param("status") GroupStatus status, @Param("updateTime") Date updateTime);
}
