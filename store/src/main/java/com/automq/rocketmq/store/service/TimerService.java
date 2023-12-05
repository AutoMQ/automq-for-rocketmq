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

import com.automq.rocketmq.common.ServiceThread;
import com.automq.rocketmq.store.exception.StoreErrorCode;
import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.model.generated.TimerHandlerType;
import com.automq.rocketmq.store.model.generated.TimerTag;
import com.automq.rocketmq.store.model.kv.BatchDeleteRequest;
import com.automq.rocketmq.store.model.kv.BatchWriteRequest;
import com.automq.rocketmq.store.service.api.KVService;
import com.aventrix.jnanoid.jnanoid.NanoIdUtils;
import com.google.common.base.Ticker;
import com.google.flatbuffers.FlatBufferBuilder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class TimerService extends ServiceThread {
    private static final Logger log = LoggerFactory.getLogger(TimerService.class);

    private final String timerTagNamespace;
    private final String timerIndexNamespace;
    private final KVService kvService;
    private final Ticker ticker;

    private final ConcurrentMap<Short /*TimerHandlerType*/, Consumer<TimerTag>> timerHandlerMap = new ConcurrentHashMap<>();

    private static final Consumer<TimerTag> DEFAULT_HANDLER = (receiptHandle) -> log.warn("No handler for timer tag: {}", receiptHandle);

    public TimerService(String namespace, KVService kvService) {
        this.timerTagNamespace = namespace + "_tag";
        this.timerIndexNamespace = namespace + "_index";
        this.kvService = kvService;
        this.ticker = new Ticker() {
            @Override
            public long read() {
                return System.currentTimeMillis();
            }
        };
    }

    public TimerService(String namespace, KVService kvService, Ticker ticker) {
        this.timerTagNamespace = namespace + "_tag";
        this.timerIndexNamespace = namespace + "_index";
        this.kvService = kvService;
        this.ticker = ticker;
    }

    @Override
    public String getServiceName() {
        return "TimerService";
    }

    public void clear() throws StoreException {
        kvService.clear(timerTagNamespace);
    }

    // All handler should not do any blocking operation.
    public void registerHandler(short handlerType, Consumer<TimerTag> handler) throws StoreException {
        if (handlerType < 0 || handlerType >= TimerHandlerType.names.length) {
            throw new StoreException(StoreErrorCode.ILLEGAL_ARGUMENT, "Invalid timer tag type: " + handlerType);
        }

        timerHandlerMap.put(handlerType, handler);
    }

    public void unregisterHandler(short handlerType) {
        timerHandlerMap.remove(handlerType);
    }

    public boolean hasHandler(short handlerType) {
        return timerHandlerMap.containsKey(handlerType);
    }

    protected byte[] buildTimerTagKey(long deliveryTimestamp, byte[] key) {
        ByteBuffer buffer = ByteBuffer.allocate(8 + key.length);
        buffer.putLong(deliveryTimestamp);
        buffer.put(key);
        return buffer.array();
    }

    private byte[] buildTimerTagValue(long deliveryTimestamp, byte[] identity, short handlerType, byte[] payload) {
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int identityOffset = builder.createByteVector(identity);
        int payloadOffset = builder.createByteVector(payload);
        int root = TimerTag.createTimerTag(builder, deliveryTimestamp, identityOffset, handlerType, payloadOffset);
        builder.finish(root);
        return builder.sizedByteArray();
    }

    private byte[] buildTimerIndexValue(long deliveryTimestamp) {
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(deliveryTimestamp).array();
    }

    public String enqueue(long deliveryTimestamp, short handlerType, byte[] payload) throws StoreException {
        String identity = NanoIdUtils.randomNanoId();
        enqueue(deliveryTimestamp, identity.getBytes(StandardCharsets.UTF_8), handlerType, payload);
        return identity;
    }

    private void checkHandler(short handlerType) throws StoreException {
        if (handlerType < 0 || handlerType >= TimerHandlerType.names.length) {
            throw new StoreException(StoreErrorCode.ILLEGAL_ARGUMENT, "Invalid timer tag type: " + handlerType);
        }

        if (!timerHandlerMap.containsKey(handlerType)) {
            throw new StoreException(StoreErrorCode.ILLEGAL_ARGUMENT, "No handler for timer tag type: " + TimerHandlerType.name(handlerType));
        }
    }

    public void enqueue(long deliveryTimestamp, byte[] identity, short handlerType,
        byte[] payload) throws StoreException {
        checkHandler(handlerType);

        byte[] key = buildTimerTagKey(deliveryTimestamp, identity);
        byte[] value = buildTimerTagValue(deliveryTimestamp, identity, handlerType, payload);

        BatchWriteRequest writeTagRequest = new BatchWriteRequest(timerTagNamespace, key, value);
        BatchWriteRequest writeIndexRequest = new BatchWriteRequest(timerIndexNamespace, identity, buildTimerIndexValue(deliveryTimestamp));
        kvService.batch(writeTagRequest, writeIndexRequest);
    }

    public BatchWriteRequest enqueueRequest(long deliveryTimestamp, byte[] identity, short handlerType,
        byte[] payload) {
        return new BatchWriteRequest(timerTagNamespace, buildTimerTagKey(deliveryTimestamp, identity), buildTimerTagValue(deliveryTimestamp, identity, handlerType, payload));
    }

    public boolean cancel(byte[] identity) throws StoreException {
        byte[] value = kvService.get(timerIndexNamespace, identity);
        if (value == null || value.length != Long.SIZE / Byte.SIZE) {
            return false;
        }

        long deliveryTimestamp = ByteBuffer.wrap(value).getLong();
        BatchDeleteRequest deleteTagRequest = new BatchDeleteRequest(timerTagNamespace, buildTimerTagKey(deliveryTimestamp, identity));
        BatchDeleteRequest deleteIndexRequest = new BatchDeleteRequest(timerIndexNamespace, identity);
        kvService.batch(deleteTagRequest, deleteIndexRequest);
        return true;
    }

    public List<BatchDeleteRequest> cancelRequest(long deliveryTimestamp, byte[] identity) {
        BatchDeleteRequest deleteTagRequest = new BatchDeleteRequest(timerTagNamespace, buildTimerTagKey(deliveryTimestamp, identity));
        BatchDeleteRequest deleteIndexRequest = new BatchDeleteRequest(timerIndexNamespace, identity);

        return List.of(deleteTagRequest, deleteIndexRequest);
    }

    public Optional<TimerTag> get(byte[] identity) throws StoreException {
        byte[] value = kvService.get(timerIndexNamespace, identity);
        if (value == null || value.length != Long.SIZE / Byte.SIZE) {
            return Optional.empty();
        }

        long deliveryTimestamp = ByteBuffer.wrap(value).getLong();
        byte[] tagValue = kvService.get(timerTagNamespace, buildTimerTagKey(deliveryTimestamp, identity));
        if (tagValue == null) {
            return Optional.empty();
        }

        TimerTag timerTag = TimerTag.getRootAsTimerTag(ByteBuffer.wrap(tagValue));
        return Optional.of(timerTag);
    }

    @Override
    public void run() {
        while (!this.isStopped()) {
            try {
                dequeue();
            } catch (StoreException e) {
                log.error("Failed to dequeue timer tag", e);
            }
            waitForRunning(100);
        }
    }

    protected void dequeue() throws StoreException {
        byte[] start = ByteBuffer.allocate(8).putLong(0).array();
        long endTimestamp = ticker.read() + 1;
        byte[] end = ByteBuffer.allocate(8).putLong(endTimestamp).array();

        // Iterate timer tag until now to find messages need to reconsume.
        kvService.iterate(timerTagNamespace, null, start, end, (key, value) -> {
            // Fetch the origin message from stream store.
            TimerTag timerTag = TimerTag.getRootAsTimerTag(ByteBuffer.wrap(value));
            try {
                timerHandlerMap.getOrDefault(timerTag.handlerType(), DEFAULT_HANDLER).accept(timerTag);
            } catch (Exception e) {
                log.error("Failed to handle timer tag: {}", timerTag, e);
            }

            try {
                ByteBuffer buffer = timerTag.identityAsByteBuffer();
                byte[] identity = new byte[buffer.remaining()];
                buffer.get(identity);
                BatchDeleteRequest deleteTagRequest = new BatchDeleteRequest(timerTagNamespace, key);
                BatchDeleteRequest deleteIndexRequest = new BatchDeleteRequest(timerIndexNamespace, identity);
                kvService.batch(deleteTagRequest, deleteIndexRequest);
            } catch (StoreException e) {
                log.error("Failed to delete timer tag: {}", timerTag, e);
            }
        });
    }
}
