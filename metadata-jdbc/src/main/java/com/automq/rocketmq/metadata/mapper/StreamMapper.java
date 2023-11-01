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

import apache.rocketmq.controller.v1.StreamState;
import com.automq.rocketmq.metadata.dao.Stream;
import com.automq.rocketmq.metadata.dao.StreamCriteria;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface StreamMapper {

    int create(Stream stream);

    Stream getByStreamId(long id);

    int increaseEpoch(long id);

    int updateLastRange(@Param("id") long id, @Param("lastRangeId") int lastRangeId);

    int updateStreamState(@Param("criteria") StreamCriteria criteria, @Param("state") StreamState state);

    int delete(@Param("id") Long id);

    List<Stream> byCriteria(@Param("criteria") StreamCriteria criteria);

    void update(Stream stream);

    long queueEpoch(@Param("topicId") long topicId,
        @Param("queueId") long queueId);


    int planMove(@Param("criteria") StreamCriteria criteria,
        @Param("srcNodeId") int src,
        @Param("dstNodeId") int dst,
        @Param("state") StreamState state);
}
