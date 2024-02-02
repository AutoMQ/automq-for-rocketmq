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

package com.automq.rocketmq.store.api;

import com.automq.rocketmq.common.model.generated.FlatMessage;
import com.automq.rocketmq.common.trace.TraceContext;
import java.util.concurrent.CompletableFuture;

public interface DeadLetterSender {
    /**
     * Send message to dead letter topic.
     *
     * @param context trace context
     * @param consumerGroupId consumer group id
     * @param message original message
     */
    CompletableFuture<Void> send(TraceContext context, long consumerGroupId, FlatMessage message);
}
