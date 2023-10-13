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

import com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject;
import org.apache.ibatis.annotations.Param;

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
}
