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

import com.automq.rocketmq.metadata.dao.S3StreamObject;
import org.apache.ibatis.annotations.Param;

import java.util.Date;
import java.util.List;

public interface S3StreamObjectMapper {

    int create(S3StreamObject s3StreamObject);

    List<S3StreamObject> list(@Param("objectId") Long objectId,
        @Param("streamId") Long streamId,
        @Param("startOffset") Long startOffset,
        @Param("endOffset") Long endOffset,
        @Param("limit") Integer limit);

    S3StreamObject getById(long id);

    List<S3StreamObject> listByStreamId(long streamId);

    List<S3StreamObject> listByObjectId(long objectId);

    int commit(S3StreamObject s3StreamObject);

    S3StreamObject getByStreamAndObject(@Param("streamId") long streamId, @Param("objectId") long objectId);

    int delete(@Param("id") Long id, @Param("streamId") Long streamId, @Param("objectId") Long objectId);

    S3StreamObject getByObjectId(long objectId);

    int batchDelete(@Param("objectIds") List<Long> objectIds);

    List<S3StreamObject> recyclable(@Param("streamIds") List<Long> streamIds, @Param("threshold") Date threshold);
}
