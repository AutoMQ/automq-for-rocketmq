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

package com.automq.rocketmq.metadata.service;

import apache.rocketmq.controller.v1.SubStreams;
import com.automq.rocketmq.common.system.S3Constants;
import com.automq.rocketmq.metadata.dao.S3StreamSetObject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

public final class Helper {

    public static apache.rocketmq.controller.v1.S3StreamSetObject buildS3StreamSetObject(S3StreamSetObject s3StreamSetObject) throws InvalidProtocolBufferException {
        return buildS3StreamSetObject(s3StreamSetObject, decode(s3StreamSetObject.getSubStreams()));
    }

    static apache.rocketmq.controller.v1.S3StreamSetObject buildS3StreamSetObject(S3StreamSetObject s3StreamSetObject, SubStreams subStreams) {
        return apache.rocketmq.controller.v1.S3StreamSetObject.newBuilder()
            .setObjectId(s3StreamSetObject.getObjectId())
            .setObjectSize(s3StreamSetObject.getObjectSize())
            .setBrokerId(s3StreamSetObject.getNodeId())
            .setSequenceId(s3StreamSetObject.getSequenceId())
            .setBaseDataTimestamp(s3StreamSetObject.getBaseDataTimestamp().getTime())
            .setCommittedTimestamp(s3StreamSetObject.getCommittedTimestamp() != null ?
                s3StreamSetObject.getCommittedTimestamp().getTime() : S3Constants.NOOP_OBJECT_COMMIT_TIMESTAMP)
            .setSubStreams(subStreams)
            .build();
    }

    static SubStreams decode(String json) throws InvalidProtocolBufferException {
        SubStreams.Builder builder = SubStreams.newBuilder();
        JsonFormat.parser().ignoringUnknownFields().merge(json, builder);
        return builder.build();
    }
}
