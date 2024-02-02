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

import java.util.Objects;

public class TopicQueueId {

    private final long topicId;
    private final int queueId;

    public TopicQueueId(long topicId, int queueId) {
        this.topicId = topicId;
        this.queueId = queueId;
    }

    public static TopicQueueId of(long topicId, int queueId) {
        return new TopicQueueId(topicId, queueId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        TopicQueueId id = (TopicQueueId) o;
        return topicId == id.topicId && queueId == id.queueId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicId, queueId);
    }

    @Override
    public String toString() {
        return "TopicQueueId{" +
            "topicId=" + topicId +
            ", queueId=" + queueId +
            '}';
    }
}
