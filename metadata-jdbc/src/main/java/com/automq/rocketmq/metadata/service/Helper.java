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
