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

package com.automq.rocketmq.controller.metadata.database;

import apache.rocketmq.controller.v1.Code;
import apache.rocketmq.controller.v1.CommitStreamObjectRequest;
import apache.rocketmq.controller.v1.CommitWALObjectRequest;
import apache.rocketmq.controller.v1.PrepareS3ObjectsReply;
import apache.rocketmq.controller.v1.PrepareS3ObjectsRequest;
import apache.rocketmq.controller.v1.S3ObjectState;
import apache.rocketmq.controller.v1.S3StreamObject;
import apache.rocketmq.controller.v1.S3WALObject;
import apache.rocketmq.controller.v1.StreamState;
import apache.rocketmq.controller.v1.SubStream;
import apache.rocketmq.controller.v1.TrimStreamRequest;
import com.automq.rocketmq.common.system.S3Constants;
import com.automq.rocketmq.common.system.StreamConstants;
import com.automq.rocketmq.controller.exception.ControllerException;
import com.automq.rocketmq.controller.metadata.MetadataStore;
import com.automq.rocketmq.controller.metadata.database.dao.Range;
import com.automq.rocketmq.controller.metadata.database.dao.S3Object;
import com.automq.rocketmq.controller.metadata.database.dao.S3WalObject;
import com.automq.rocketmq.controller.metadata.database.dao.Stream;
import com.automq.rocketmq.controller.metadata.database.mapper.RangeMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.S3ObjectMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.S3StreamObjectMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.S3WalObjectMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.SequenceMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.StreamMapper;
import com.automq.rocketmq.controller.metadata.database.serde.SubStreamDeserializer;
import com.automq.rocketmq.controller.metadata.database.serde.SubStreamSerializer;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.TextFormat;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3MetadataManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3MetadataManager.class);

    private final MetadataStore metadataStore;

    private final Gson gson;

    public S3MetadataManager(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
        this.gson = new GsonBuilder()
            .registerTypeAdapter(SubStream.class, new SubStreamSerializer())
            .registerTypeAdapter(SubStream.class, new SubStreamDeserializer())
            .create();
    }

    public CompletableFuture<Long> prepareS3Objects(int count, int ttlInMinutes) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        for (; ; ) {
            if (metadataStore.isLeader()) {
                try (SqlSession session = metadataStore.openSession()) {
                    if (!metadataStore.maintainLeadershipWithSharedLock(session)) {
                        continue;
                    }

                    // Get and update sequence
                    SequenceMapper sequenceMapper = session.getMapper(SequenceMapper.class);
                    long next = sequenceMapper.next(S3ObjectMapper.SEQUENCE_NAME);
                    sequenceMapper.update(S3ObjectMapper.SEQUENCE_NAME, next + count);

                    S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
                    Calendar calendar = Calendar.getInstance();
                    calendar.add(Calendar.MINUTE, ttlInMinutes);
                    IntStream.range(0, count).forEach(i -> {
                        S3Object object = new S3Object();
                        object.setId(next + i);
                        object.setState(S3ObjectState.BOS_PREPARED);
                        object.setExpiredTimestamp(calendar.getTime());
                        s3ObjectMapper.prepare(object);
                    });
                    session.commit();
                    future.complete(next);
                }
            } else {
                PrepareS3ObjectsRequest request = PrepareS3ObjectsRequest.newBuilder()
                    .setPreparedCount(count)
                    .setTimeToLiveMinutes(ttlInMinutes)
                    .build();
                try {
                    metadataStore.controllerClient().prepareS3Objects(metadataStore.leaderAddress(), request)
                        .thenApply(PrepareS3ObjectsReply::getFirstObjectId);
                } catch (ControllerException e) {
                    future.completeExceptionally(e);
                }
            }
            break;
        }
        return future;
    }

    public CompletableFuture<Void> commitWalObject(S3WALObject walObject,
        List<S3StreamObject> streamObjects, List<Long> compactedObjects) {
        if (Objects.isNull(walObject)) {
            LOGGER.error("S3WALObject is unexpectedly null");
            ControllerException e = new ControllerException(Code.INTERNAL_VALUE, "S3WALObject is unexpectedly null");
            return CompletableFuture.failedFuture(e);
        }

        LOGGER.info("commitWalObject with walObject: {}, streamObjects: {}, compactedObjects: {}",
            TextFormat.shortDebugString(walObject),
            streamObjects.stream()
                .map(TextFormat::shortDebugString)
                .collect(Collectors.joining()), compactedObjects
        );

        CompletableFuture<Void> future = new CompletableFuture<>();
        for (; ; ) {
            if (metadataStore.isLeader()) {
                try (SqlSession session = metadataStore.openSession()) {
                    if (!metadataStore.maintainLeadershipWithSharedLock(session)) {
                        continue;
                    }

                    S3WalObjectMapper s3WALObjectMapper = session.getMapper(S3WalObjectMapper.class);
                    S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
                    S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);

                    int brokerId = walObject.getBrokerId();
                    long objectId = walObject.getObjectId();

                    if (Objects.isNull(compactedObjects) || compactedObjects.isEmpty()) {
                        // verify stream continuity
                        List<long[]> offsets = java.util.stream.Stream.concat(
                            streamObjects.stream()
                                .map(s3StreamObject -> new long[] {s3StreamObject.getStreamId(), s3StreamObject.getStartOffset(), s3StreamObject.getEndOffset()}),
                            walObject.getSubStreamsMap().entrySet()
                                .stream()
                                .map(obj -> new long[] {obj.getKey(), obj.getValue().getStartOffset(), obj.getValue().getEndOffset()})
                        ).toList();

                        if (!checkStreamAdvance(session, offsets)) {
                            LOGGER.error("S3WALObject[object-id={}]'s stream advance check failed", walObject.getObjectId());
                            ControllerException e = new ControllerException(Code.NOT_FOUND_VALUE, String.format("S3WALObject[object-id=%d]'s stream advance check failed", walObject.getObjectId()));
                            future.completeExceptionally(e);
                            return future;
                        }
                    }

                    // commit S3 object
                    if (objectId != S3Constants.NOOP_OBJECT_ID && !commitObject(objectId, StreamConstants.NOOP_STREAM_ID, walObject.getObjectSize(), session)) {
                        ControllerException e = new ControllerException(Code.ILLEGAL_STATE_VALUE,
                            String.format("S3WALObject[object-id=%d] is not ready for commit", walObject.getObjectId()));
                        future.completeExceptionally(e);
                        return future;
                    }

                    long dataTs = System.currentTimeMillis();
                    long sequenceId = objectId;
                    if (!Objects.isNull(compactedObjects) && !compactedObjects.isEmpty()) {
                        List<S3WalObject> s3WalObjects = compactedObjects.stream()
                            .map(id -> {
                                // mark destroy compacted object
                                S3Object object = s3ObjectMapper.getById(id);
                                object.setState(S3ObjectState.BOS_WILL_DELETE);
                                object.setMarkedForDeletionTimestamp(new Date());
                                s3ObjectMapper.markToDelete(object.getId(), new Date());

                                return s3WALObjectMapper.getByObjectId(id);
                            })
                            .toList();

                        if (!s3WalObjects.isEmpty()) {
                            // update dataTs to the min compacted object's dataTs
                            dataTs = s3WalObjects.stream()
                                .map(S3WalObject::getBaseDataTimestamp)
                                .map(Date::getTime)
                                .min(Long::compareTo).get();
                            // update sequenceId to the min compacted object's sequenceId
                            sequenceId = s3WalObjects.stream().mapToLong(S3WalObject::getSequenceId).min().getAsLong();
                        }
                    }

                    // commit stream objects;
                    if (!streamObjects.isEmpty()) {
                        for (apache.rocketmq.controller.v1.S3StreamObject s3StreamObject : streamObjects) {
                            long oId = s3StreamObject.getObjectId();
                            long objectSize = s3StreamObject.getObjectSize();
                            long streamId = s3StreamObject.getStreamId();
                            if (!commitObject(oId, streamId, objectSize, session)) {
                                ControllerException e = new ControllerException(Code.ILLEGAL_STATE_VALUE, String.format("S3StreamObject[object-id=%d] is not ready for commit", oId));
                                future.completeExceptionally(e);
                                return future;
                            }
                        }
                        // create stream object records
                        streamObjects.forEach(s3StreamObject -> {
                            com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject object = new com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject();
                            object.setStreamId(s3StreamObject.getStreamId());
                            object.setObjectId(s3StreamObject.getObjectId());
                            object.setCommittedTimestamp(new Date());
                            object.setStartOffset(s3StreamObject.getStartOffset());
                            object.setBaseDataTimestamp(new Date(s3StreamObject.getBaseDataTimestamp()));
                            object.setEndOffset(s3StreamObject.getEndOffset());
                            object.setObjectSize(s3StreamObject.getObjectSize());
                            s3StreamObjectMapper.commit(object);
                        });
                    }

                    // generate compacted objects' remove record ...
                    if (!Objects.isNull(compactedObjects) && !compactedObjects.isEmpty()) {
                        compactedObjects.forEach(id -> s3WALObjectMapper.delete(id, null, null));
                    }

                    // update broker's wal object
                    if (objectId != S3Constants.NOOP_OBJECT_ID) {
                        // generate broker's wal object record
                        S3WalObject s3WALObject = new S3WalObject();
                        s3WALObject.setObjectId(objectId);
                        s3WALObject.setObjectSize(walObject.getObjectSize());
                        s3WALObject.setBaseDataTimestamp(new Date(dataTs));
                        s3WALObject.setCommittedTimestamp(new Date());
                        s3WALObject.setNodeId(brokerId);
                        s3WALObject.setSequenceId(sequenceId);
                        s3WALObject.setSubStreams(gson.toJson(walObject.getSubStreamsMap()));
                        s3WALObjectMapper.create(s3WALObject);
                    }

                    session.commit();
                    LOGGER.info("broker[broke-id={}] commit wal object[object-id={}] success, compacted objects[{}], stream objects[{}]",
                        brokerId, walObject.getObjectId(), compactedObjects, streamObjects);
                    future.complete(null);
                }
            } else {
                CommitWALObjectRequest request = CommitWALObjectRequest.newBuilder()
                    .setS3WalObject(walObject)
                    .addAllS3StreamObjects(streamObjects)
                    .addAllCompactedObjectIds(compactedObjects)
                    .build();
                try {
                    metadataStore.controllerClient().commitWALObject(metadataStore.leaderAddress(), request).whenComplete(((reply, e) -> {
                        if (null != e) {
                            future.completeExceptionally(e);
                        } else {
                            future.complete(null);
                        }
                    }));
                } catch (ControllerException e) {
                    future.completeExceptionally(e);
                }
            }
            break;
        }
        return future;
    }

    public CompletableFuture<Void> commitStreamObject(apache.rocketmq.controller.v1.S3StreamObject streamObject,
        List<Long> compactedObjects) throws ControllerException {

        LOGGER.info("commitStreamObject with streamObject: {}, compactedObjects: {}", TextFormat.shortDebugString(streamObject),
            compactedObjects);

        CompletableFuture<Void> future = new CompletableFuture<>();
        for (; ; ) {
            if (metadataStore.isLeader()) {
                try (SqlSession session = metadataStore.openSession()) {
                    if (!metadataStore.maintainLeadershipWithSharedLock(session)) {
                        continue;
                    }
                    if (streamObject.getObjectId() == S3Constants.NOOP_OBJECT_ID) {
                        LOGGER.error("S3StreamObject[object-id={}] is null or objectId is unavailable", streamObject.getObjectId());
                        ControllerException e = new ControllerException(Code.NOT_FOUND_VALUE, String.format("S3StreamObject[object-id=%d] is null or objectId is unavailable", streamObject.getObjectId()));
                        future.completeExceptionally(e);
                        return future;
                    }

                    long committedTs = System.currentTimeMillis();
                    S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
                    S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);

                    // commit object
                    if (!commitObject(streamObject.getObjectId(), streamObject.getStreamId(), streamObject.getObjectSize(), session)) {
                        ControllerException e = new ControllerException(Code.ILLEGAL_STATE_VALUE, String.format("S3StreamObject[object-id=%d] is not ready for commit", streamObject.getObjectId()));
                        future.completeExceptionally(e);
                        return future;
                    }
                    long dataTs = committedTs;
                    if (!Objects.isNull(compactedObjects) && !compactedObjects.isEmpty()) {
                        dataTs = compactedObjects.stream()
                            .map(id -> {
                                // mark destroy compacted object
                                S3Object object = s3ObjectMapper.getById(id);
                                object.setState(S3ObjectState.BOS_WILL_DELETE);
                                object.setMarkedForDeletionTimestamp(new Date());
                                s3ObjectMapper.markToDelete(object.getId(), new Date());

                                // update dataTs to the min compacted object's dataTs
                                com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject s3StreamObject = s3StreamObjectMapper.getByObjectId(id);
                                return s3StreamObject.getBaseDataTimestamp().getTime();
                            })
                            .min(Long::compareTo).get();
                    }
                    // create a new S3StreamObject to replace committed ones
                    if (streamObject.getObjectId() != S3Constants.NOOP_OBJECT_ID) {
                        com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject newS3StreamObj = new com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject();
                        newS3StreamObj.setStreamId(streamObject.getStreamId());
                        newS3StreamObj.setObjectId(streamObject.getObjectId());
                        newS3StreamObj.setObjectSize(streamObject.getObjectSize());
                        newS3StreamObj.setStartOffset(streamObject.getStartOffset());
                        newS3StreamObj.setEndOffset(streamObject.getEndOffset());
                        newS3StreamObj.setBaseDataTimestamp(new Date(dataTs));
                        newS3StreamObj.setCommittedTimestamp(new Date(committedTs));
                        s3StreamObjectMapper.create(newS3StreamObj);
                    }

                    // delete the compactedObjects of S3Stream
                    if (!Objects.isNull(compactedObjects) && !compactedObjects.isEmpty()) {
                        compactedObjects.forEach(id -> s3StreamObjectMapper.delete(null, null, id));
                    }
                    session.commit();
                    LOGGER.info("S3StreamObject[object-id={}] commit success, compacted objects: {}", streamObject.getObjectId(), compactedObjects);
                    future.complete(null);
                }
            } else {
                CommitStreamObjectRequest request = CommitStreamObjectRequest.newBuilder()
                    .setS3StreamObject(streamObject)
                    .addAllCompactedObjectIds(compactedObjects)
                    .build();
                try {
                    metadataStore.controllerClient().commitStreamObject(metadataStore.leaderAddress(), request).whenComplete(((reply, e) -> {
                        if (null != e) {
                            future.completeExceptionally(e);
                        } else {
                            future.complete(null);
                        }
                    }));
                } catch (ControllerException e) {
                    future.completeExceptionally(e);
                }
            }
            break;
        }
        return future;
    }

    public CompletableFuture<List<S3WALObject>> listWALObjects() {
        CompletableFuture<List<S3WALObject>> future = new CompletableFuture<>();
        try (SqlSession session = metadataStore.openSession()) {
            S3WalObjectMapper s3WalObjectMapper = session.getMapper(S3WalObjectMapper.class);
            List<S3WALObject> walObjects = s3WalObjectMapper.list(metadataStore.config().nodeId(), null).stream()
                .map(s3WALObject -> {
                    Map<Long, SubStream> subStreams = gson.fromJson(new String(s3WALObject.getSubStreams().getBytes(StandardCharsets.UTF_8)),
                        new TypeToken<>() {
                        });
                    return buildS3WALObject(s3WALObject, subStreams);
                })
                .toList();
            future.complete(walObjects);
        }
        return future;
    }

    public CompletableFuture<List<S3WALObject>> listWALObjects(long streamId, long startOffset,
        long endOffset, int limit) {
        CompletableFuture<List<S3WALObject>> future = new CompletableFuture<>();
        try (SqlSession session = metadataStore.openSession()) {
            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);

            List<Integer> nodes = rangeMapper.listByStreamId(streamId)
                .stream()
                .filter(range -> range.getEndOffset() > startOffset && range.getStartOffset() < endOffset)
                .mapToInt(Range::getNodeId)
                .distinct()
                .boxed()
                .toList();

            S3WalObjectMapper s3WalObjectMapper = session.getMapper(S3WalObjectMapper.class);
            List<S3WALObject> s3WALObjects = new ArrayList<>();
            for (int nodeId : nodes) {
                List<S3WalObject> s3WalObjects = s3WalObjectMapper.list(nodeId, null);
                s3WalObjects.stream()
                    .map(s3WalObject -> {
                        TypeToken<Map<Long, SubStream>> typeToken = new TypeToken<>() {
                        };
                        Map<Long, SubStream> subStreams = gson.fromJson(new String(s3WalObject.getSubStreams().getBytes(StandardCharsets.UTF_8)), typeToken);
                        Map<Long, SubStream> streamsRecords = new HashMap<>();
                        if (!Objects.isNull(subStreams) && subStreams.containsKey(streamId)) {
                            SubStream subStream = subStreams.get(streamId);
                            if (subStream.getStartOffset() <= endOffset && subStream.getEndOffset() > startOffset) {
                                streamsRecords.put(streamId, subStream);
                            }
                        }
                        if (!streamsRecords.isEmpty()) {
                            return buildS3WALObject(s3WalObject, streamsRecords);
                        }
                        return null;
                    })
                    .filter(Objects::nonNull)
                    .forEach(s3WALObjects::add);
            }

            // Sort by start-offset of the given stream
            s3WALObjects.sort((l, r) -> {
                long lhs = l.getSubStreamsMap().get(streamId).getStartOffset();
                long rhs = r.getSubStreamsMap().get(streamId).getStartOffset();
                return Long.compare(lhs, rhs);
            });

            future.complete(s3WALObjects.stream().limit(limit).toList());
        }
        return future;
    }

    public CompletableFuture<List<apache.rocketmq.controller.v1.S3StreamObject>> listStreamObjects(long streamId,
        long startOffset, long endOffset, int limit) {
        CompletableFuture<List<apache.rocketmq.controller.v1.S3StreamObject>> future = new CompletableFuture<>();
        try (SqlSession session = metadataStore.openSession()) {
            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            List<apache.rocketmq.controller.v1.S3StreamObject> streamObjects = s3StreamObjectMapper.list(null, streamId, startOffset, endOffset, limit).stream()
                .map(this::buildS3StreamObject)
                .toList();
            future.complete(streamObjects);
        }
        return future;
    }

    private S3StreamObject buildS3StreamObject(
        com.automq.rocketmq.controller.metadata.database.dao.S3StreamObject originalObject) {
        return S3StreamObject.newBuilder()
            .setStreamId(originalObject.getStreamId())
            .setObjectSize(originalObject.getObjectSize())
            .setObjectId(originalObject.getObjectId())
            .setStartOffset(originalObject.getStartOffset())
            .setEndOffset(originalObject.getEndOffset())
            .setBaseDataTimestamp(originalObject.getBaseDataTimestamp().getTime())
            .setCommittedTimestamp(originalObject.getCommittedTimestamp() != null ? originalObject.getCommittedTimestamp().getTime() : S3Constants.NOOP_OBJECT_COMMIT_TIMESTAMP)
            .build();
    }

    private S3WALObject buildS3WALObject(
        S3WalObject originalObject,
        Map<Long, SubStream> subStreams) {
        return S3WALObject.newBuilder()
            .setObjectId(originalObject.getObjectId())
            .setObjectSize(originalObject.getObjectSize())
            .setBrokerId(originalObject.getNodeId())
            .setSequenceId(originalObject.getSequenceId())
            .setBaseDataTimestamp(originalObject.getBaseDataTimestamp().getTime())
            .setCommittedTimestamp(originalObject.getCommittedTimestamp() != null ? originalObject.getCommittedTimestamp().getTime() : S3Constants.NOOP_OBJECT_COMMIT_TIMESTAMP)
            .putAllSubStreams(subStreams)
            .build();
    }

    private boolean commitObject(Long objectId, long streamId, long objectSize, SqlSession session) {
        S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
        S3Object s3Object = s3ObjectMapper.getById(objectId);
        if (Objects.isNull(s3Object)) {
            LOGGER.error("object[object-id={}] not exist", objectId);
            return false;
        }
        // verify the state
        if (s3Object.getState() == S3ObjectState.BOS_COMMITTED) {
            LOGGER.warn("object[object-id={}] already committed", objectId);
            return false;
        }
        if (s3Object.getState() != S3ObjectState.BOS_PREPARED) {
            LOGGER.error("object[object-id={}] is not prepared but try to commit", objectId);
            return false;
        }

        Date commitData = new Date();
        if (s3Object.getExpiredTimestamp().getTime() < commitData.getTime()) {
            LOGGER.error("object[object-id={}] is expired", objectId);
            return false;
        }

        s3Object.setCommittedTimestamp(commitData);
        s3Object.setStreamId(streamId);
        s3Object.setObjectSize(objectSize);
        s3Object.setState(S3ObjectState.BOS_COMMITTED);
        s3ObjectMapper.commit(s3Object);
        return true;
    }

    private boolean checkStreamAdvance(SqlSession session, List<long[]> offsets) {
        if (offsets == null || offsets.isEmpty()) {
            return true;
        }
        StreamMapper streamMapper = session.getMapper(StreamMapper.class);
        RangeMapper rangeMapper = session.getMapper(RangeMapper.class);
        for (long[] offset : offsets) {
            long streamId = offset[0], startOffset = offset[1], endOffset = offset[2];
            // verify the stream exists and is open
            Stream stream = streamMapper.getByStreamId(streamId);
            if (stream.getState() != StreamState.OPEN) {
                LOGGER.warn("Stream[stream-id={}] not opened", streamId);
                return false;
            }

            Range range = rangeMapper.get(stream.getRangeId(), streamId, null);
            if (Objects.isNull(range)) {
                // should not happen
                LOGGER.error("Stream[stream-id={}]'s current range[range-id={}] not exist when stream has been created",
                    streamId, stream.getRangeId());
                return false;
            }

            if (range.getEndOffset() != startOffset) {
                LOGGER.warn("Stream[stream-id={}]'s current range[range-id={}]'s end offset[{}] is not equal to request start offset[{}]",
                    streamId, range.getRangeId(), range.getEndOffset(), startOffset);
                return false;
            }

            range.setEndOffset(endOffset);
            rangeMapper.update(range);
        }
        return true;
    }

    public CompletableFuture<Pair<List<S3StreamObject>, List<S3WALObject>>> listObjects(
        long streamId, long startOffset, long endOffset, int limit) {
        return CompletableFuture.supplyAsync(() -> {
            try (SqlSession session = metadataStore.openSession()) {
                S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
                S3WalObjectMapper s3WalObjectMapper = session.getMapper(S3WalObjectMapper.class);
                List<apache.rocketmq.controller.v1.S3StreamObject> s3StreamObjects = s3StreamObjectMapper.list(null, streamId, startOffset, endOffset, limit)
                    .stream()
                    .map(this::buildS3StreamObject)
                    .toList();

                List<S3WALObject> walObjects = new ArrayList<>();
                s3WalObjectMapper.list(null, null)
                    .stream()
                    .map(s3WalObject -> {
                        TypeToken<Map<Long, SubStream>> typeToken = new TypeToken<>() {
                        };
                        Map<Long, SubStream> subStreams = gson.fromJson(new String(s3WalObject.getSubStreams().getBytes(StandardCharsets.UTF_8)), typeToken);
                        Map<Long, SubStream> streamsRecords = new HashMap<>();
                        subStreams.entrySet().stream()
                            .filter(entry -> !Objects.isNull(entry) && entry.getKey().equals(streamId))
                            .filter(entry -> entry.getValue().getStartOffset() <= endOffset && entry.getValue().getEndOffset() > startOffset)
                            .forEach(entry -> streamsRecords.put(entry.getKey(), entry.getValue()));
                        return streamsRecords.isEmpty() ? null : buildS3WALObject(s3WalObject, streamsRecords);
                    })
                    .filter(Objects::nonNull)
                    .limit(limit)
                    .forEach(walObjects::add);

                if (!walObjects.isEmpty()) {
                    walObjects.sort((l, r) -> {
                        long lhs = l.getSubStreamsMap().get(streamId).getStartOffset();
                        long rhs = r.getSubStreamsMap().get(streamId).getStartOffset();
                        return Long.compare(lhs, rhs);
                    });
                }

                // apply limit in whole.
                Set<Long> objectIds = java.util.stream.Stream.concat(
                        s3StreamObjects.stream()
                            .map(s3StreamObject -> new long[] {
                                s3StreamObject.getObjectId(),
                                s3StreamObject.getStartOffset(),
                                s3StreamObject.getEndOffset()
                            }),
                        walObjects.stream()
                            .map(s3WALObject -> new long[] {
                                s3WALObject.getObjectId(),
                                s3WALObject.getSubStreamsMap().get(streamId).getStartOffset(),
                                s3WALObject.getSubStreamsMap().get(streamId).getEndOffset()
                            })
                    ).sorted((l, r) -> {
                        if (l[1] == r[1]) {
                            return Long.compare(l[0], r[0]);
                        }
                        return Long.compare(l[1], r[1]);
                    }).limit(limit)
                    .map(offset -> offset[0])
                    .collect(Collectors.toSet());

                List<apache.rocketmq.controller.v1.S3StreamObject> limitedStreamObjects = s3StreamObjects.stream()
                    .filter(s3StreamObject -> objectIds.contains(s3StreamObject.getObjectId()))
                    .toList();

                List<S3WALObject> limitedWalObjectList = walObjects.stream()
                    .filter(s3WALObject -> objectIds.contains(s3WALObject.getObjectId()))
                    .toList();

                return new ImmutablePair<>(limitedStreamObjects, limitedWalObjectList);
            }
        }, metadataStore.asyncExecutor());
    }


    public CompletableFuture<Void> trimStream(long streamId, long streamEpoch,
        long newStartOffset) throws ControllerException {
        CompletableFuture<Void> future = new CompletableFuture<>();
        for (; ; ) {
            if (metadataStore.isLeader()) {
                try (SqlSession session = metadataStore.openSession()) {
                    if (!metadataStore.maintainLeadershipWithSharedLock(session)) {
                        continue;
                    }
                    StreamMapper streamMapper = session.getMapper(StreamMapper.class);
                    RangeMapper rangeMapper = session.getMapper(RangeMapper.class);
                    S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
                    S3WalObjectMapper s3WALObjectMapper = session.getMapper(S3WalObjectMapper.class);
                    S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);

                    Stream stream = streamMapper.getByStreamId(streamId);
                    if (null == stream) {
                        ControllerException e = new ControllerException(Code.NOT_FOUND_VALUE,
                            String.format("Stream[stream-id=%d] is not found", streamId)
                        );
                        future.completeExceptionally(e);
                        return future;
                    }
                    if (stream.getState() == StreamState.CLOSED) {
                        LOGGER.warn("Stream[{}]‘s state is CLOSED, can't trim", streamId);
                        return null;
                    }
                    if (stream.getStartOffset() > newStartOffset) {
                        LOGGER.warn("Stream[{}]‘s start offset {} is larger than request new start offset {}",
                            streamId, stream.getStartOffset(), newStartOffset);
                        return null;
                    }
                    if (stream.getStartOffset() == newStartOffset) {
                        // regard it as redundant trim operation, just return success
                        return null;
                    }

                    // now the request is valid
                    // update the stream metadata start offset
                    stream.setEpoch(streamEpoch);
                    stream.setStartOffset(newStartOffset);
                    streamMapper.update(stream);

                    // remove range or update range's start offset
                    rangeMapper.listByStreamId(streamId).forEach(range -> {
                        if (newStartOffset <= range.getStartOffset()) {
                            return;
                        }
                        if (stream.getRangeId().equals(range.getRangeId())) {
                            // current range, update start offset
                            // if current range is [50, 100)
                            // 1. try to trim to 40, then current range will be [50, 100)
                            // 2. try to trim to 60, then current range will be [60, 100)
                            // 3. try to trim to 100, then current range will be [100, 100)
                            // 4. try to trim to 110, then current range will be [100, 100)
                            long newRangeStartOffset = newStartOffset < range.getEndOffset() ? newStartOffset : range.getEndOffset();
                            range.setStartOffset(newRangeStartOffset);
                            rangeMapper.update(range);
                            return;
                        }
                        if (newStartOffset >= range.getEndOffset()) {
                            // remove range
                            rangeMapper.delete(range.getRangeId(), streamId);
                            return;
                        }
                        // update range's start offset
                        range.setStartOffset(newStartOffset);
                        rangeMapper.update(range);
                    });
                    // remove stream object
                    s3StreamObjectMapper.listByStreamId(streamId).forEach(streamObject -> {
                        long streamStartOffset = streamObject.getStartOffset();
                        long streamEndOffset = streamObject.getEndOffset();
                        if (newStartOffset <= streamStartOffset) {
                            return;
                        }
                        if (newStartOffset >= streamEndOffset) {
                            // stream object
                            s3StreamObjectMapper.delete(null, streamId, streamObject.getObjectId());
                            // markDestroyObjects
                            S3Object s3Object = s3ObjectMapper.getById(streamObject.getObjectId());
                            s3Object.setMarkedForDeletionTimestamp(new Date());
                            s3ObjectMapper.markToDelete(s3Object.getId(), new Date());
                        }
                    });

                    // remove wal object or remove sub-stream range in wal object
                    s3WALObjectMapper.list(stream.getDstNodeId(), null).stream()
                        .map(s3WALObject -> {
                            Map<Long, SubStream> subStreams = gson.fromJson(new String(s3WALObject.getSubStreams().getBytes(StandardCharsets.UTF_8)),
                                new TypeToken<>() {
                                });
                            return buildS3WALObject(s3WALObject, subStreams);
                        })
                        .filter(s3WALObject -> s3WALObject.getSubStreamsMap().containsKey(streamId))
                        .filter(s3WALObject -> s3WALObject.getSubStreamsMap().get(streamId).getEndOffset() <= newStartOffset)
                        .forEach(s3WALObject -> {
                            if (s3WALObject.getSubStreamsMap().size() == 1) {
                                // only this range, but we will remove this range, so now we can remove this wal object
                                S3Object s3Object = s3ObjectMapper.getById(s3WALObject.getObjectId());
                                s3Object.setMarkedForDeletionTimestamp(new Date());
                                s3ObjectMapper.markToDelete(s3Object.getId(), new Date());
                            }

                            // remove offset range about sub-stream ...
                        });
                    session.commit();
                    LOGGER.info("Node[node-id={}] trim stream [stream-id={}] with epoch={} and newStartOffset={}",
                        metadataStore.config().nodeId(), streamId, streamEpoch, newStartOffset);
                    future.complete(null);
                }
            } else {
                TrimStreamRequest request = TrimStreamRequest.newBuilder()
                    .setStreamId(streamId)
                    .setStreamEpoch(streamEpoch)
                    .setNewStartOffset(newStartOffset)
                    .build();
                try {
                    metadataStore.controllerClient().trimStream(metadataStore.leaderAddress(), request).whenComplete(((reply, e) -> {
                        if (null != e) {
                            future.completeExceptionally(e);
                        } else {
                            future.complete(null);
                        }
                    }));
                } catch (ControllerException e) {
                    future.completeExceptionally(e);
                }
            }
            break;
        }
        return future;
    }
}
