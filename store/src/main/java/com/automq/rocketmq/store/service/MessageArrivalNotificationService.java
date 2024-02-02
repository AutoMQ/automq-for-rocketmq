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

package com.automq.rocketmq.store.service;

import apache.rocketmq.controller.v1.Topic;
import com.automq.rocketmq.store.api.MessageArrivalListener;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MessageArrivalNotificationService {
    ConcurrentLinkedQueue<MessageArrivalListener> listenerQueue = new ConcurrentLinkedQueue<>();

    public void registerMessageArriveListener(MessageArrivalListener listener) {
        listenerQueue.add(listener);
    }

    public void notify(MessageArrivalListener.MessageSource source, Topic topic, int queueId, long offset, String tag) {
        for (MessageArrivalListener listener : listenerQueue) {
            listener.apply(source, topic, queueId, offset, tag);
        }
    }
}
