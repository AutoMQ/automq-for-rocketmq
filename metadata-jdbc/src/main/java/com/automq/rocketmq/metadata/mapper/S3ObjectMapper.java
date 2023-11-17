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

import apache.rocketmq.controller.v1.S3ObjectState;
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

    List<S3Object> list(@Param("state") S3ObjectState state, @Param("streamId") Long streamId);

    int prepare(S3Object s3Object);

    int rollback(@Param("current")Date current);

    long totalDataSize(@Param("streamId") long streamId);
}
