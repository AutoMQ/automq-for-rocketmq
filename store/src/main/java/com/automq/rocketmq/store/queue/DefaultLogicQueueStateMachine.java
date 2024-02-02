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

package com.automq.rocketmq.store.queue;

import com.automq.rocketmq.store.MessageStoreImpl;
import com.automq.rocketmq.store.api.MessageStateMachine;
import com.automq.rocketmq.store.exception.StoreErrorCode;
import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.model.generated.CheckPoint;
import com.automq.rocketmq.store.model.generated.TimerHandlerType;
import com.automq.rocketmq.store.model.kv.BatchDeleteRequest;
import com.automq.rocketmq.store.model.kv.BatchRequest;
import com.automq.rocketmq.store.model.kv.BatchWriteRequest;
import com.automq.rocketmq.store.model.metadata.ConsumerGroupMetadata;
import com.automq.rocketmq.store.model.operation.AckOperation;
import com.automq.rocketmq.store.model.operation.ChangeInvisibleDurationOperation;
import com.automq.rocketmq.store.model.operation.OperationSnapshot;
import com.automq.rocketmq.store.model.operation.PopOperation;
import com.automq.rocketmq.store.model.operation.ResetConsumeOffsetOperation;
import com.automq.rocketmq.store.service.TimerService;
import com.automq.rocketmq.store.service.api.KVService;
import com.automq.rocketmq.store.util.SerializeUtil;
import com.automq.stream.utils.FutureUtil;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.automq.rocketmq.store.MessageStoreImpl.KV_NAMESPACE_CHECK_POINT;
import static com.automq.rocketmq.store.MessageStoreImpl.KV_NAMESPACE_FIFO_INDEX;
import static com.automq.rocketmq.store.util.SerializeUtil.buildCheckPointKey;
import static com.automq.rocketmq.store.util.SerializeUtil.buildCheckPointValue;
import static com.automq.rocketmq.store.util.SerializeUtil.buildOrderIndexKey;
import static com.automq.rocketmq.store.util.SerializeUtil.buildOrderIndexValue;
import static com.automq.rocketmq.store.util.SerializeUtil.buildReceiptHandle;
import static com.automq.rocketmq.store.util.SerializeUtil.buildReceiptHandleKey;

public class DefaultLogicQueueStateMachine implements MessageStateMachine {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultLogicQueueStateMachine.class);
    private final long topicId;
    private final int queueId;
    private ConcurrentMap<Long/*consumerGroup*/, ConsumerGroupMetadata> consumerGroupMetadataMap;
    private long currentOperationOffset = -1;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock reentrantLock = lock.readLock();
    private final Lock exclusiveLock = lock.writeLock();
    private final KVService kvService;
    private final TimerService timerService;
    private final String identity;
    private final List<OffsetListener> retryAckOffsetListeners = new ArrayList<>();

    public DefaultLogicQueueStateMachine(long topicId, int queueId, KVService kvService, TimerService timerService) {
        this.consumerGroupMetadataMap = new ConcurrentHashMap<>();
        this.kvService = kvService;
        this.timerService = timerService;
        this.topicId = topicId;
        this.queueId = queueId;
        this.identity = "[DefaultStateMachine-" + topicId + "-" + queueId + "]";
    }

    @Override
    public long topicId() {
        return topicId;
    }

    @Override
    public int queueId() {
        return queueId;
    }

    @Override
    public ReplayPopResult replayPopOperation(long operationOffset, PopOperation operation) throws StoreException {
        reentrantLock.lock();
        try {
            this.currentOperationOffset = operationOffset;
            return switch (operation.popOperationType()) {
                case POP_NORMAL -> replayPopNormalOperation(operationOffset, operation);
                case POP_ORDER -> replayPopFifoOperation(operationOffset, operation);
                case POP_RETRY -> replayPopRetryOperation(operationOffset, operation);
            };
        } finally {
            reentrantLock.unlock();
        }
    }

    private ReplayPopResult replayPopNormalOperation(long operationOffset,
        PopOperation operation) throws StoreException {
        long topicId = operation.topicId();
        int queueId = operation.queueId();
        long offset = operation.offset();
        long consumerGroupId = operation.consumerGroupId();
        long operationId = operationOffset;
        long operationTimestamp = operation.operationTimestamp();
        long nextVisibleTimestamp = operation.operationTimestamp() + operation.invisibleDuration();
        int count = operation.count();

        LOGGER.trace("Replay pop normal operation: topicId={}, queueId={}, offset={}, consumerGroupId={}, operationId={}, operationTimestamp={}, nextVisibleTimestamp={} at offset: {}",
            topicId, queueId, offset, consumerGroupId, operationId, operationTimestamp, nextVisibleTimestamp, operationOffset);

        // update consume offset, data or retry stream
        ConsumerGroupMetadata metadata = this.consumerGroupMetadataMap.computeIfAbsent(consumerGroupId, k -> new ConsumerGroupMetadata(consumerGroupId));
        if (metadata.getConsumeOffset() < offset + 1) {
            // if this is a pop-last operation, it only needs to update consume offset
            metadata.setConsumeOffset(offset + 1);
        }
        if (operation.isEndMark()) {
            return ReplayPopResult.empty();
        }

        List<BatchRequest> requestList = new ArrayList<>();
        // write a ck for this offset
        BatchWriteRequest writeCheckPointRequest = new BatchWriteRequest(KV_NAMESPACE_CHECK_POINT,
            buildCheckPointKey(topicId, queueId, consumerGroupId, operationId),
            buildCheckPointValue(topicId, queueId, offset,
                count,
                consumerGroupId, operationId, operation.popOperationType(), operationTimestamp, nextVisibleTimestamp));
        requestList.add(writeCheckPointRequest);

        BatchWriteRequest timerEnqueueRequest = timerService.enqueueRequest(
            nextVisibleTimestamp, buildReceiptHandleKey(topicId, queueId, operationId),
            TimerHandlerType.POP_REVIVE, buildReceiptHandle(consumerGroupId, topicId, queueId, operationId));
        requestList.add(timerEnqueueRequest);

        kvService.batch(requestList.toArray(new BatchRequest[0]));
        // normal pop operation does not need to update consume times
        return ReplayPopResult.of(1);
    }

    private ReplayPopResult replayPopRetryOperation(long operationOffset,
        PopOperation operation) throws StoreException {
        long topicId = operation.topicId();
        int queueId = operation.queueId();
        long offset = operation.offset();
        long consumerGroupId = operation.consumerGroupId();
        long operationId = operationOffset;
        long operationTimestamp = operation.operationTimestamp();
        long nextVisibleTimestamp = operation.operationTimestamp() + operation.invisibleDuration();
        int count = operation.count();

        LOGGER.trace("Replay pop retry operation: topicId={}, queueId={}, offset={}, consumerGroupId={}, operationId={}, operationTimestamp={}, nextVisibleTimestamp={} at offset: {}",
            topicId, queueId, offset, consumerGroupId, operationId, operationTimestamp, nextVisibleTimestamp, operationOffset);

        // update consume offset, data or retry stream
        ConsumerGroupMetadata metadata = this.consumerGroupMetadataMap.computeIfAbsent(consumerGroupId, k -> new ConsumerGroupMetadata(consumerGroupId));
        metadata.advanceRetryConsumeOffset(offset + 1);

        List<BatchRequest> requestList = new ArrayList<>();
        // write a ck for this offset
        BatchWriteRequest writeCheckPointRequest = new BatchWriteRequest(KV_NAMESPACE_CHECK_POINT,
            buildCheckPointKey(topicId, queueId, consumerGroupId, operationId),
            buildCheckPointValue(topicId, queueId, offset,
                count,
                consumerGroupId, operationId, operation.popOperationType(), operationTimestamp, nextVisibleTimestamp));
        requestList.add(writeCheckPointRequest);

        BatchWriteRequest timerEnqueueRequest = timerService.enqueueRequest(
            nextVisibleTimestamp, buildReceiptHandleKey(topicId, queueId, operationId),
            TimerHandlerType.POP_REVIVE, buildReceiptHandle(consumerGroupId, topicId, queueId, operationId));
        requestList.add(timerEnqueueRequest);

        kvService.batch(requestList.toArray(new BatchRequest[0]));
        return ReplayPopResult.empty();
    }

    private ReplayPopResult replayPopFifoOperation(long operationOffset,
        PopOperation operation) throws StoreException {
        long topicId = operation.topicId();
        int queueId = operation.queueId();
        long offset = operation.offset();
        long consumerGroupId = operation.consumerGroupId();
        long operationId = operationOffset;
        long operationTimestamp = operation.operationTimestamp();
        long nextVisibleTimestamp = operation.operationTimestamp() + operation.invisibleDuration();

        int count = operation.count();

        LOGGER.trace("Replay pop fifo operation: topicId={}, queueId={}, offset={}, consumerGroupId={}, operationId={}, operationTimestamp={}, nextVisibleTimestamp={} at offset: {}",
            topicId, queueId, offset, consumerGroupId, operationId, operationTimestamp, nextVisibleTimestamp, operationOffset);

        // update consume offset
        ConsumerGroupMetadata metadata = this.consumerGroupMetadataMap.computeIfAbsent(consumerGroupId, k -> new ConsumerGroupMetadata(consumerGroupId));
        if (metadata.getConsumeOffset() < offset + 1) {
            metadata.setConsumeOffset(offset + 1);
        }
        if (operation.isEndMark()) {
            // if this is a pop-last operation, it only needs to update consume offset
            return ReplayPopResult.empty();
        }

        List<BatchRequest> requestList = new ArrayList<>();
        // write a ck for this offset
        BatchWriteRequest writeCheckPointRequest = new BatchWriteRequest(KV_NAMESPACE_CHECK_POINT,
            buildCheckPointKey(topicId, queueId, consumerGroupId, operationId),
            buildCheckPointValue(topicId, queueId, offset,
                count,
                consumerGroupId, operationId, operation.popOperationType(), operationTimestamp, nextVisibleTimestamp));
        requestList.add(writeCheckPointRequest);

        BatchWriteRequest timerEnqueueRequest = timerService.enqueueRequest(
            nextVisibleTimestamp, buildReceiptHandleKey(topicId, queueId, operationId),
            TimerHandlerType.POP_REVIVE, buildReceiptHandle(consumerGroupId, topicId, queueId, operationId));
        requestList.add(timerEnqueueRequest);

        // if this message is orderly, write order index for each offset in this operation to KV service
        long baseOffset = offset - count + 1;
        for (int i = 0; i < count; i++) {
            long currOffset = baseOffset + i;
            byte[] orderIndexKey = buildOrderIndexKey(consumerGroupId, topicId, queueId, currOffset);
            int consumeTimes = 0;
            try {
                byte[] orderIndexValue = kvService.get(KV_NAMESPACE_FIFO_INDEX, orderIndexKey);
                if (orderIndexValue != null) {
                    ByteBuffer wrappedBuffer = ByteBuffer.wrap(orderIndexValue);
                    consumeTimes = wrappedBuffer.getInt(8);
                }
            } catch (StoreException e) {
                LOGGER.error("{}: get consume times from order index failed", identity, e);
            }
            BatchWriteRequest writeOrderIndexRequest = new BatchWriteRequest(KV_NAMESPACE_FIFO_INDEX, orderIndexKey, buildOrderIndexValue(operationId, consumeTimes + 1));
            requestList.add(writeOrderIndexRequest);
        }

        kvService.batch(requestList.toArray(new BatchRequest[0]));
        return ReplayPopResult.of(consumeTimes(consumerGroupId, offset));
    }

    @Override
    public void replayAckOperation(long operationOffset, AckOperation operation) throws StoreException {
        long topicId = operation.topicId();
        int queueId = operation.queueId();
        long operationId = operation.operationId();
        long consumerGroupId = operation.consumerGroupId();
        AckOperation.AckOperationType type = operation.ackOperationType();

        LOGGER.trace("Replay ack operation: topicId={}, queueId={}, operationId={}, type={} at offset: {}",
            topicId, queueId, operationId, type, operationOffset);

        reentrantLock.lock();
        try {
            currentOperationOffset = operationOffset;
            // check if this ack is stale
            ConsumerGroupMetadata metadata = this.consumerGroupMetadataMap.computeIfAbsent(consumerGroupId, k -> new ConsumerGroupMetadata(consumerGroupId));
            if (metadata.getVersion() > operationId) {
                LOGGER.info("{}: Ack operation is stale, ignore it. topicId={}, queueId={}, operationId={}, type={} at offset: {}",
                    identity, topicId, queueId, operationId, type, operationOffset);
                return;
            }
            // check if ck exists
            byte[] ckKey = buildCheckPointKey(topicId, queueId, consumerGroupId, operationId);
            byte[] ckValue = kvService.get(KV_NAMESPACE_CHECK_POINT, ckKey);
            if (ckValue == null) {
                throw new StoreException(StoreErrorCode.ILLEGAL_ARGUMENT, "Ack operation failed, check point not found");
            }
            CheckPoint ck = CheckPoint.getRootAsCheckPoint(ByteBuffer.wrap(ckValue));
            deleteCheckPointAndRewriteOrderIndex(ck);

            // Update ack offset
            long ackOffset;
            if (ck.popOperationType() == PopOperation.PopOperationType.POP_RETRY.ordinal()) {
                ackOffset = metadata.getRetryConsumeOffset();
            } else {
                ackOffset = metadata.getConsumeOffset();
            }

            byte[] prefix = kvService.getByPrefix(KV_NAMESPACE_CHECK_POINT, SerializeUtil.buildCheckPointGroupPrefix(topicId, queueId, consumerGroupId));
            if (prefix != null) {
                CheckPoint earliestCK = SerializeUtil.decodeCheckPoint(ByteBuffer.wrap(prefix));
                ackOffset = earliestCK.messageOffset();
            }

            if (ck.popOperationType() == PopOperation.PopOperationType.POP_NORMAL.ordinal() ||
                (ck.popOperationType() == PopOperation.PopOperationType.POP_ORDER.ordinal() && type == AckOperation.AckOperationType.ACK_NORMAL)) {
                metadata.advanceAckOffset(ackOffset);
            }
            if (ck.popOperationType() == PopOperation.PopOperationType.POP_RETRY.ordinal()) {
                metadata.advanceRetryAckOffset(ackOffset);
                for (OffsetListener listener : retryAckOffsetListeners) {
                    listener.onOffset(consumerGroupId, ackOffset);
                }
            }
        } finally {
            reentrantLock.unlock();
        }
    }

    @Override
    public void replayChangeInvisibleDurationOperation(long operationOffset,
        ChangeInvisibleDurationOperation operation) {
        long invisibleDuration = operation.invisibleDuration();
        long operationTimestamp = operation.operationTimestamp();
        long topic = operation.topicId();
        int queue = operation.queueId();
        long operationId = operation.operationId();
        long consumerGroupId = operation.consumerGroupId();
        long nextVisibleTimestamp = operationTimestamp + invisibleDuration;

        LOGGER.trace("Replay change invisible duration operation: topicId={}, queueId={}, operationId={}, invisibleDuration={}, operationTimestamp={}, nextVisibleTimestamp={} at offset: {}",
            topic, queue, operationId, invisibleDuration, operationTimestamp, nextVisibleTimestamp, operationOffset);

        reentrantLock.lock();
        try {
            currentOperationOffset = operationOffset;
            // Check if check point exists.
            byte[] checkPointKey = buildCheckPointKey(topic, queue, consumerGroupId, operationId);
            byte[] buffer = kvService.get(KV_NAMESPACE_CHECK_POINT, checkPointKey);
            if (buffer == null) {
                throw new StoreException(StoreErrorCode.ILLEGAL_ARGUMENT, "Change invisible duration operation failed, check point not found");
            }

            // Delete last timer tag.
            CheckPoint checkPoint = CheckPoint.getRootAsCheckPoint(ByteBuffer.wrap(buffer));
            List<BatchDeleteRequest> timerCancelRequestList = timerService.cancelRequest(checkPoint.nextVisibleTimestamp(),
                buildReceiptHandleKey(checkPoint.topicId(), checkPoint.queueId(), checkPoint.operationId()));
            List<BatchRequest> requestList = new ArrayList<>(timerCancelRequestList);

            // Write new check point and timer tag.
            BatchWriteRequest writeCheckPointRequest = new BatchWriteRequest(KV_NAMESPACE_CHECK_POINT,
                buildCheckPointKey(checkPoint.topicId(), checkPoint.queueId(), checkPoint.consumerGroupId(), checkPoint.operationId()),
                buildCheckPointValue(checkPoint.topicId(), checkPoint.queueId(), checkPoint.messageOffset(), checkPoint.count(),
                    checkPoint.consumerGroupId(), checkPoint.operationId(), PopOperation.PopOperationType.valueOf(checkPoint.popOperationType()),
                    checkPoint.deliveryTimestamp(), nextVisibleTimestamp));
            requestList.add(writeCheckPointRequest);

            BatchWriteRequest timerEnqueueRequest = timerService.enqueueRequest(
                nextVisibleTimestamp, buildReceiptHandleKey(checkPoint.topicId(), checkPoint.queueId(), checkPoint.operationId()),
                TimerHandlerType.POP_REVIVE, buildReceiptHandle(checkPoint.consumerGroupId(), checkPoint.topicId(), checkPoint.queueId(), checkPoint.operationId()));
            requestList.add(timerEnqueueRequest);

            kvService.batch(requestList.toArray(new BatchRequest[0]));
        } catch (StoreException e) {
            LOGGER.error("{}: Replay change invisible duration operation failed", identity, e);
            CompletableFuture.failedFuture(e);
            return;
        } finally {
            reentrantLock.unlock();
        }
        CompletableFuture.completedFuture(null);
    }

    @Override
    public void replayResetConsumeOffsetOperation(long operationOffset, ResetConsumeOffsetOperation operation) {
        long consumerGroupId = operation.consumerGroupId();
        long newConsumeOffset = operation.offset();
        long topicId = operation.topicId();
        int queueId = operation.queueId();
        long operationTimestamp = operation.operationTimestamp();

        LOGGER.trace("Replay reset consume offset operation: topicId={}, queueId={}, consumerGroupId={}, newConsumeOffset={}, operationTimestamp={} at offset: {}",
            topicId, queueId, consumerGroupId, newConsumeOffset, operationTimestamp, operationOffset);
        reentrantLock.lock();
        try {
            currentOperationOffset = operationOffset;
            // Create a new consumer group with a new version.
            ConsumerGroupMetadata metadata = this.consumerGroupMetadataMap.computeIfAbsent(consumerGroupId, k -> new ConsumerGroupMetadata(consumerGroupId));
            ConsumerGroupMetadata newMetadata = new ConsumerGroupMetadata(
                metadata.getConsumerGroupId(), newConsumeOffset, newConsumeOffset, metadata.getRetryConsumeOffset(), metadata.getRetryAckOffset(), operationOffset);
            this.consumerGroupMetadataMap.put(consumerGroupId, newMetadata);

            // Delete all check points and related states about this consumer group
            List<CheckPoint> checkPoints = new ArrayList<>();
            byte[] prefix = SerializeUtil.buildCheckPointGroupPrefix(topicId, queueId, consumerGroupId);
            kvService.iterate(KV_NAMESPACE_CHECK_POINT, prefix, null, null, (key, value) -> {
                CheckPoint checkPoint = SerializeUtil.decodeCheckPoint(ByteBuffer.wrap(value));
                checkPoints.add(checkPoint);
            });
            deleteCheckPointsAndRelatedStates(checkPoints);
        } catch (StoreException e) {
            LOGGER.error("{}: Replay reset consume offset operation failed", identity, e);
            CompletableFuture.failedFuture(e);
        } finally {
            reentrantLock.unlock();
        }

    }

    private void deleteCheckPointsAndRelatedStates(List<CheckPoint> checkPointList) throws StoreException {
        List<BatchRequest> batchRequests = checkPointList.stream().map(this::deleteCheckPointAndRelatedStatesReqs).flatMap(List::stream).toList();
        if (!batchRequests.isEmpty()) {
            kvService.batch(batchRequests.toArray(new BatchRequest[0]));
        }
    }

    private void deleteCheckPointAndRewriteOrderIndex(CheckPoint checkPoint) throws StoreException {
        List<BatchRequest> requestList = new ArrayList<>();

        BatchDeleteRequest deleteCheckPointRequest = new BatchDeleteRequest(KV_NAMESPACE_CHECK_POINT,
            buildCheckPointKey(checkPoint.topicId(), checkPoint.queueId(), checkPoint.consumerGroupId(), checkPoint.operationId()));
        requestList.add(deleteCheckPointRequest);

        List<BatchDeleteRequest> timerCancelRequest = timerService.cancelRequest(checkPoint.nextVisibleTimestamp(),
            buildReceiptHandleKey(checkPoint.topicId(), checkPoint.queueId(), checkPoint.operationId()));
        requestList.addAll(timerCancelRequest);

        if (checkPoint.popOperationType() == PopOperation.PopOperationType.POP_ORDER.value()) {
            long baseOffset = checkPoint.messageOffset() - checkPoint.count() + 1;
            for (int i = 0; i < checkPoint.count(); i++) {
                long currOffset = baseOffset + i;
                byte[] orderIndexKey = buildOrderIndexKey(checkPoint.consumerGroupId(), checkPoint.topicId(), checkPoint.queueId(), currOffset);
                int consumeTimes = 0;
                try {
                    byte[] orderIndexValue = kvService.get(KV_NAMESPACE_FIFO_INDEX, orderIndexKey);
                    if (orderIndexValue != null) {
                        ByteBuffer wrappedBuffer = ByteBuffer.wrap(orderIndexValue);
                        consumeTimes = wrappedBuffer.getInt(8);
                    }
                } catch (StoreException e) {
                    LOGGER.error("{}: get consume times from order index failed", identity, e);
                }
                BatchWriteRequest rewriteOrderIndex = new BatchWriteRequest(KV_NAMESPACE_FIFO_INDEX, orderIndexKey, buildOrderIndexValue(-1, consumeTimes));
                requestList.add(rewriteOrderIndex);
            }
        }

        kvService.batch(requestList.toArray(new BatchRequest[0]));
    }

    private List<BatchRequest> deleteCheckPointAndRelatedStatesReqs(CheckPoint checkPoint) {
        List<BatchRequest> requestList = new ArrayList<>();

        BatchDeleteRequest deleteCheckPointRequest = new BatchDeleteRequest(KV_NAMESPACE_CHECK_POINT,
            buildCheckPointKey(checkPoint.topicId(), checkPoint.queueId(), checkPoint.consumerGroupId(), checkPoint.operationId()));
        requestList.add(deleteCheckPointRequest);

        List<BatchDeleteRequest> timerCancelRequest = timerService.cancelRequest(checkPoint.nextVisibleTimestamp(),
            buildReceiptHandleKey(checkPoint.topicId(), checkPoint.queueId(), checkPoint.operationId()));
        requestList.addAll(timerCancelRequest);

        if (checkPoint.popOperationType() == PopOperation.PopOperationType.POP_ORDER.value()) {
            long baseOffset = checkPoint.messageOffset() - checkPoint.count() + 1;
            for (int i = 0; i < checkPoint.count(); i++) {
                long currOffset = baseOffset + i;
                BatchDeleteRequest deleteOrderIndexRequest = new BatchDeleteRequest(KV_NAMESPACE_FIFO_INDEX,
                    buildOrderIndexKey(checkPoint.consumerGroupId(), checkPoint.topicId(), checkPoint.queueId(), currOffset));
                requestList.add(deleteOrderIndexRequest);
            }
        }
        return requestList;
    }

    public void writeCheckPointsAndRelatedStates(List<CheckPoint> checkPointList) throws StoreException {
        List<BatchRequest> batchRequests = checkPointList.stream()
            .map(this::writeCheckPointAndRelatedStatesReqs)
            .flatMap(List::stream)
            .toList();
        if (!batchRequests.isEmpty()) {
            kvService.batch(batchRequests.toArray(new BatchRequest[0]));
        }
    }

    private void writeCheckPointAndRelatedStates(CheckPoint checkPoint) throws StoreException {
        List<BatchRequest> batchRequests = writeCheckPointAndRelatedStatesReqs(checkPoint);
        if (!batchRequests.isEmpty()) {
            kvService.batch(batchRequests.toArray(new BatchRequest[0]));
        }
    }

    private List<BatchRequest> writeCheckPointAndRelatedStatesReqs(CheckPoint checkPoint) {
        List<BatchRequest> requestList = new ArrayList<>();
        // write ck
        BatchWriteRequest writeCheckPointRequest = new BatchWriteRequest(KV_NAMESPACE_CHECK_POINT,
            buildCheckPointKey(checkPoint.topicId(), checkPoint.queueId(), checkPoint.consumerGroupId(), checkPoint.operationId()),
            buildCheckPointValue(checkPoint.topicId(), checkPoint.queueId(), checkPoint.messageOffset(), checkPoint.count(),
                checkPoint.consumerGroupId(), checkPoint.operationId(), PopOperation.PopOperationType.valueOf(checkPoint.popOperationType()),
                checkPoint.deliveryTimestamp(), checkPoint.nextVisibleTimestamp()));
        requestList.add(writeCheckPointRequest);
        // write timer tag

        BatchWriteRequest timerEnqueueRequest = timerService.enqueueRequest(
            checkPoint.nextVisibleTimestamp(), buildReceiptHandleKey(checkPoint.topicId(), checkPoint.queueId(), checkPoint.operationId()),
            TimerHandlerType.POP_REVIVE, buildReceiptHandle(checkPoint.consumerGroupId(), checkPoint.topicId(), checkPoint.queueId(), checkPoint.operationId()));
        requestList.add(timerEnqueueRequest);
        // write order index
        if (checkPoint.popOperationType() == PopOperation.PopOperationType.POP_ORDER.ordinal()) {
            long baseOffset = checkPoint.messageOffset() - checkPoint.count() + 1;
            for (int i = 0; i < checkPoint.count(); i++) {
                long currOffset = baseOffset + i;
                byte[] orderIndexKey = buildOrderIndexKey(checkPoint.consumerGroupId(), checkPoint.topicId(), checkPoint.queueId(), currOffset);
                int consumeTimes = 0;
                try {
                    byte[] orderIndexValue = kvService.get(KV_NAMESPACE_FIFO_INDEX, orderIndexKey);
                    if (orderIndexValue != null) {
                        ByteBuffer wrappedBuffer = ByteBuffer.wrap(orderIndexValue);
                        consumeTimes = wrappedBuffer.getInt(8);
                    }
                } catch (StoreException e) {
                    LOGGER.error("{}: get consume times from order index failed", identity, e);
                }
                BatchWriteRequest writeOrderIndexRequest = new BatchWriteRequest(KV_NAMESPACE_FIFO_INDEX, orderIndexKey, buildOrderIndexValue(checkPoint.operationId(), consumeTimes + 1));
                requestList.add(writeOrderIndexRequest);
            }
        }
        return requestList;
    }

    @Override
    public OperationSnapshot takeSnapshot() throws StoreException {
        exclusiveLock.lock();
        try {
            List<ConsumerGroupMetadata> metadataSnapshots = consumerGroupMetadataMap.values()
                .stream()
                .map(metadata -> new ConsumerGroupMetadata(metadata.getConsumerGroupId(), metadata.getConsumeOffset(), metadata.getAckOffset(),
                    metadata.getRetryConsumeOffset(), metadata.getRetryAckOffset(), metadata.getVersion()))
                .collect(Collectors.toList());
            long snapshotVersion = kvService.takeSnapshot();
            return new OperationSnapshot(currentOperationOffset, snapshotVersion, metadataSnapshots);
        } finally {
            exclusiveLock.unlock();
        }
    }

    @Override
    public void loadSnapshot(OperationSnapshot snapshot) {
        exclusiveLock.lock();
        try {
            this.consumerGroupMetadataMap = snapshot.getConsumerGroupMetadataList().stream().collect(Collectors.toConcurrentMap(
                ConsumerGroupMetadata::getConsumerGroupId, metadataSnapshot ->
                    new ConsumerGroupMetadata(metadataSnapshot.getConsumerGroupId(), metadataSnapshot.getConsumeOffset(), metadataSnapshot.getAckOffset(),
                        metadataSnapshot.getRetryConsumeOffset(), metadataSnapshot.getRetryAckOffset(), metadataSnapshot.getVersion())));
            this.currentOperationOffset = snapshot.getSnapshotEndOffset();
        } catch (Exception e) {
            Throwable cause = FutureUtil.cause(e);
            LOGGER.error("{}: Load snapshot:{} failed", identity, snapshot, cause);
            CompletableFuture.failedFuture(e);
            return;
        } finally {
            exclusiveLock.unlock();
        }
        CompletableFuture.completedFuture(null);
    }

    @Override
    public void clear() throws StoreException {
        exclusiveLock.lock();
        try {
            this.consumerGroupMetadataMap.clear();
            this.currentOperationOffset = -1;
            List<CheckPoint> checkPointList = new ArrayList<>();
            byte[] tqPrefix = SerializeUtil.buildCheckPointQueuePrefix(topicId, queueId);
            kvService.iterate(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, tqPrefix, null, null, (key, value) -> {
                CheckPoint checkPoint = SerializeUtil.decodeCheckPoint(ByteBuffer.wrap(value));
                checkPointList.add(checkPoint);
            });
            deleteCheckPointsAndRelatedStates(checkPointList);
        } finally {
            exclusiveLock.unlock();
        }
    }

    @Override
    public long consumeOffset(long consumerGroupId) {
        return consumerGroupMetadataMap.computeIfAbsent(consumerGroupId, k -> new ConsumerGroupMetadata(consumerGroupId))
            .getConsumeOffset();
    }

    @Override
    public long ackOffset(long consumerGroupId) {
        return consumerGroupMetadataMap.computeIfAbsent(consumerGroupId, k -> new ConsumerGroupMetadata(consumerGroupId))
            .getAckOffset();
    }

    @Override
    public long retryConsumeOffset(long consumerGroupId) {
        return consumerGroupMetadataMap.computeIfAbsent(consumerGroupId, k -> new ConsumerGroupMetadata(consumerGroupId))
            .getRetryConsumeOffset();
    }

    @Override
    public long retryAckOffset(long consumerGroupId) {
        return consumerGroupMetadataMap.computeIfAbsent(consumerGroupId, k -> new ConsumerGroupMetadata(consumerGroupId))
            .getRetryAckOffset();
    }

    @Override
    public boolean isLocked(long consumerGroupId, long offset) throws StoreException {
        exclusiveLock.lock();
        try {
            byte[] lockKey = buildOrderIndexKey(consumerGroupId, topicId, queueId, offset);
            byte[] value = kvService.get(KV_NAMESPACE_FIFO_INDEX, lockKey);
            // If the message is acked or send to dead letter topic, the order index will be cleared.
            if (value != null) {
                long operationId = ByteBuffer.wrap(value).getLong();
                // If the operation id is positive, it means that the message is locked.
                // If the message could be visible again, the ReviveService will set operation id to -1.
                return operationId >= 0;
            }
            return false;
        } finally {
            exclusiveLock.unlock();
        }
    }

    @Override
    public int consumeTimes(long consumerGroupId, long offset) {
        byte[] orderIndexKey = buildOrderIndexKey(consumerGroupId, topicId, queueId, offset);
        try {
            byte[] orderIndexValue = kvService.get(KV_NAMESPACE_FIFO_INDEX, orderIndexKey);
            if (orderIndexValue == null) {
                return 1;
            }
            ByteBuffer wrappedBuffer = ByteBuffer.wrap(orderIndexValue);
            return wrappedBuffer.getInt(8);
        } catch (StoreException e) {
            LOGGER.error("{}: get consume times from order index failed", identity, e);
        }
        return 1;
    }

    @Override
    public void registerRetryAckOffsetListener(OffsetListener listener) {
        retryAckOffsetListeners.add(listener);
    }
}
