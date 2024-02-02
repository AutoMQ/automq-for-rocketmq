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

import apache.rocketmq.controller.v1.Topic;

@FunctionalInterface
public interface MessageArrivalListener {
    /**
     * Notify message arrival.
     *
     * @param source  message source
     * @param topic   topic
     * @param queueId queue id
     * @param offset  message offset
     * @param tag     message tag
     */
    void apply(MessageSource source, Topic topic, int queueId, long offset, String tag);

    enum MessageSource {
        MESSAGE_PUT,
        RETRY_MESSAGE_PUT,
        DELAY_MESSAGE_DEQUEUE,
        TRANSACTION_MESSAGE_COMMIT,
    }
}
