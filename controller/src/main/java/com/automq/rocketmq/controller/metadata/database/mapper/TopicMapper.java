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

import apache.rocketmq.controller.v1.TopicStatus;
import com.automq.rocketmq.controller.metadata.database.dao.Topic;
import java.util.Date;
import java.util.List;
import org.apache.ibatis.annotations.Param;

public interface TopicMapper {

    int create(Topic topic);

    Topic get(@Param("id") Long id, @Param("name") String name);

    int updateStatusById(@Param("id") long id, @Param("status") TopicStatus status);

    List<Topic> list(@Param("status") TopicStatus status, @Param("updateTime") Date updateTime);

    int delete(Long id);

}
