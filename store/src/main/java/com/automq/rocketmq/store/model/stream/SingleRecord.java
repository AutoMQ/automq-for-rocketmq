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

package com.automq.rocketmq.store.model.stream;

import com.automq.stream.api.RecordBatch;
import java.nio.ByteBuffer;
import java.util.Map;

public record SingleRecord(ByteBuffer rawPayload) implements RecordBatch {
    @Override
    public int count() {
        return 1;
    }

    @Override
    public long baseTimestamp() {
        return 0;
    }

    @Override
    public Map<String, String> properties() {
        // We don't store any properties to S3Stream
        return null;
    }
}
