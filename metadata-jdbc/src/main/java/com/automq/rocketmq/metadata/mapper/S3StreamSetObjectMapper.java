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

import com.automq.rocketmq.metadata.dao.S3StreamSetObject;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface S3StreamSetObjectMapper {

    int create(S3StreamSetObject s3StreamSetObject);

    S3StreamSetObject getByObjectId(long objectId);

    int delete(@Param("objectId") Long objectId,
        @Param("nodeId") Integer nodeId,
        @Param("sequenceId") Long sequenceId);

    List<S3StreamSetObject> list(@Param("nodeId") Integer nodeId, @Param("sequenceId") Long sequenceId);

    int commit(S3StreamSetObject s3StreamSetObject);

    boolean streamExclusive(@Param("nodeId") int nodeId, @Param("streamId") long streamId);
}
