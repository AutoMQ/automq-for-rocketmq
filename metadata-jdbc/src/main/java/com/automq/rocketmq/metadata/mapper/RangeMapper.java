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

import com.automq.rocketmq.metadata.dao.Range;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface RangeMapper {

    /**
     * Create a new range record in database.
     *
     * @param range
     * @return
     */
    int create(Range range);

    /**
     *
     * @param id Primary key
     * @return Range instance
     */
    Range getById(@Param("id") long id);

    List<Range> listByStreamId(@Param("streamId") long streamId);

    void delete(@Param("rangeId") Integer rangeId, @Param("streamId") Long streamId);

    List<Range> listByNodeId(@Param("nodeId") int nodeId);

    Range get(@Param("rangeId") Integer rangeId,
        @Param("streamId") Long streamId,
        @Param("nodeId") Integer nodeId);

    List<Range> list(@Param("nodeId") Integer nodeId,
        @Param("streamId") Long streamId,
        @Param("offset") Long offset);

    void update(Range range);
}
