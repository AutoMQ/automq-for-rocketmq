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

package com.automq.rocketmq.store.util;

import com.automq.rocketmq.common.model.FlatMessageExt;
import com.automq.rocketmq.common.model.generated.FlatMessage;
import com.automq.stream.api.RecordBatchWithContext;

/**
 * An utility class to convert S3Stream Record to FlatMessage, and vice versa.
 */
public class FlatMessageUtil {
    public static FlatMessageExt transferToMessageExt(RecordBatchWithContext recordBatch) {
        FlatMessage message = FlatMessage.getRootAsFlatMessage(recordBatch.rawPayload());
        return FlatMessageExt.Builder.builder()
            .message(message)
            .offset(recordBatch.baseOffset())
            .build();
    }
}
