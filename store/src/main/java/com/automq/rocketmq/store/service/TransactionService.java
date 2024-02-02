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
