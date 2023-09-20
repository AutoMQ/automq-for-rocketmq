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

import apache.rocketmq.controller.v1.S3Object;

import java.util.List;

public interface S3ObjectMapper {

    int create(S3Object s3Object);

    S3Object getByObjectId(long objectId);

    int prepare(S3Object s3Object);

    int expired(S3Object s3Object);

    int commit(S3Object s3Object);

    int delete(long objectId);

    List<S3Object> list();
}
