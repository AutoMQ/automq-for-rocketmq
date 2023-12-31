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
