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

import apache.rocketmq.controller.v1.StreamState;
import com.automq.rocketmq.metadata.dao.Stream;
import com.automq.rocketmq.metadata.dao.StreamCriteria;
import java.util.List;
import org.apache.ibatis.annotations.Param;

public interface StreamMapper {

    int create(Stream stream);

    /**
     * For test purpose only.
     */
    int insert(Stream stream);

    Stream getByStreamId(long id);

    int increaseEpoch(long id);

    int updateLastRange(@Param("id") long id, @Param("lastRangeId") int lastRangeId);

    int updateStreamState(@Param("criteria") StreamCriteria criteria, @Param("state") StreamState state);

    int updateStreamAssignment(@Param("criteria") StreamCriteria criteria,
        @Param("srcNodeId") int src,
        @Param("dstNodeId") int dst);

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
