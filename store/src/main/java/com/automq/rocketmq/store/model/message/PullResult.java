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

public record PullResult(Status status, long nextBeginOffset, long minOffset, long maxOffset, List<FlatMessageExt> messageList) {
    public enum Status {
        FOUND,
        NO_NEW_MSG,
        NO_MATCHED_MSG,
        OFFSET_ILLEGAL
    }
}
