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

package com.automq.rocketmq.store.service;

import com.automq.rocketmq.common.config.StoreConfig;
import com.automq.rocketmq.common.model.generated.FlatMessage;
import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.model.generated.TimerHandlerType;
import com.automq.rocketmq.store.model.generated.TimerTag;
import com.automq.rocketmq.store.util.SerializeUtil;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class TransactionService {
    private static final Logger log = LoggerFactory.getLogger(TransactionService.class);

    private static final char SPLITERATOR = '@';

    private final StoreConfig config;
    private final TimerService timerService;

    public TransactionService(StoreConfig config, TimerService timerService) {
        this.config = config;
        this.timerService = timerService;
    }

    public String begin(FlatMessage message) throws StoreException {
        long duration = Math.max(config.transactionTimeoutMillis(), message.systemProperties().orphanedTransactionRecoverySeconds());
        long deliveryTimestamp = System.currentTimeMillis() + duration;

        return scheduleNextCheck(deliveryTimestamp, message);
    }

    public Optional<FlatMessage> commit(String transactionId) throws StoreException {
        Optional<TimerTag> optional = timerService.get(transactionId.getBytes(StandardCharsets.UTF_8));
        if (optional.isEmpty()) {
            return Optional.empty();
        }

        cancelCheck(transactionId);

        ByteBuffer buffer = optional.get().payloadAsByteBuffer();
        FlatMessage message = FlatMessage.getRootAsFlatMessage(buffer);
        return Optional.of(message);
    }

    public void rollback(String transactionId) throws StoreException {
        cancelCheck(transactionId);
    }

    public boolean scheduleNextCheck(FlatMessage message) throws StoreException {
        if (message.systemProperties().orphanedTransactionCheckTimes() >= config.transactionCheckMaxTimes()) {
            return false;
        }

        long deliveryTimestamp = System.currentTimeMillis() + config.transactionCheckInterval();
        scheduleNextCheck(deliveryTimestamp, message);
        return true;
    }

    protected String scheduleNextCheck(long deliveryTimestamp, FlatMessage message) throws StoreException {
        String transactionId = message.systemProperties().messageId();
        timerService.enqueue(deliveryTimestamp, transactionId.getBytes(StandardCharsets.UTF_8), TimerHandlerType.TRANSACTION_MESSAGE, SerializeUtil.flatBufferToByteArray(message));
        return transactionId;
    }

    protected void cancelCheck(String transactionId) throws StoreException {
        timerService.cancel(transactionId.getBytes(StandardCharsets.UTF_8));
    }
}
