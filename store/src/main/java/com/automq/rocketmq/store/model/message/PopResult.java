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

package com.automq.rocketmq.store.model.message;

import com.automq.rocketmq.common.model.FlatMessageExt;
import java.util.List;

public record PopResult(Status status, long deliveryTimestamp, List<FlatMessageExt> messageList,
                        long restMessageCount) {
    public enum Status {
        FOUND,
        NOT_FOUND,
        END_OF_QUEUE,
        ILLEGAL_OFFSET,
        LOCKED,
        ERROR
    }
}
