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

import com.automq.rocketmq.metadata.dao.S3Object;
import com.automq.rocketmq.metadata.dao.S3ObjectCriteria;
import java.util.Date;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface S3ObjectMapper {

    String SEQUENCE_NAME = "S3_OBJECT_ID_SEQ";

    S3Object getById(long id);

    int commit(S3Object s3Object);

    int markToDelete(@Param("id") long id, @Param("time") Date time);

    int deleteByCriteria(@Param("criteria")S3ObjectCriteria criteria);

    List<S3Object> list(@Param("criteria") S3ObjectCriteria criteria);

    int prepare(S3Object s3Object);

    long totalDataSize(@Param("streamId") long streamId);
}
