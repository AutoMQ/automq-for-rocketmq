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

import apache.rocketmq.controller.v1.StreamState;
import com.automq.rocketmq.controller.metadata.database.dao.Stream;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface StreamMapper {

    int create(Stream stream);

    Stream getByStreamId(long id);

    int increaseEpoch(long id);

    int updateLastRange(@Param("id") long id, @Param("lastRangeId") int lastRangeId);

    int updateStreamState(@Param("id") Long id, @Param("topicId") Long topicId, @Param("queueId") Integer queueId,
        @Param("state") StreamState state);

    int delete(@Param("id") Long id);

    List<Stream> list(@Param("topicId") Long topicId, @Param("queueId") Integer queueId, @Param("groupId") Long groupId);

    List<Stream> listByNode(@Param("nodeId") int nodeId, @Param("state") StreamState state);

    void update(Stream stream);
}
