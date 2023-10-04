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

import com.automq.rocketmq.controller.metadata.database.dao.Range;
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

    Range getById(long id);

    List<Range> listByRangeId(int rangeId);

    List<Range> listByStreamId(long streamId);

    void delete(@Param("rangeId") Integer rangeId, @Param("streamId") Long streamId);

    List<Range> listByBrokerId(int brokerId);

    Range get(@Param("rangeId") Integer rangeId,
        @Param("streamId") Long streamId,
        @Param("brokerId") Integer brokerId);

    List<Range> list(@Param("brokerId") Integer brokerId,
        @Param("streamId") Long streamId,
        @Param("offset") Long offset);

    void update(Range range);
}
