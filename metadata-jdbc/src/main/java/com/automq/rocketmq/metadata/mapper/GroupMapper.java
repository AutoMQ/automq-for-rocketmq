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
