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
