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

package com.automq.rocketmq.metadata.service;

import apache.rocketmq.common.v1.Code;
import apache.rocketmq.controller.v1.S3ObjectState;
import apache.rocketmq.controller.v1.S3StreamObject;
import apache.rocketmq.controller.v1.S3StreamSetObject;
import apache.rocketmq.controller.v1.StreamState;
import apache.rocketmq.controller.v1.SubStream;
import apache.rocketmq.controller.v1.SubStreams;
import com.automq.rocketmq.common.config.ControllerConfig;
import com.automq.rocketmq.common.exception.ControllerException;
import com.automq.rocketmq.common.system.S3Constants;
import com.automq.rocketmq.common.system.StreamConstants;
import com.automq.rocketmq.metadata.dao.Range;
import com.automq.rocketmq.metadata.dao.S3Object;
import com.automq.rocketmq.metadata.dao.Stream;
import com.automq.rocketmq.metadata.mapper.RangeMapper;
import com.automq.rocketmq.metadata.mapper.S3ObjectMapper;
import com.automq.rocketmq.metadata.mapper.S3StreamObjectMapper;
import com.automq.rocketmq.metadata.mapper.S3StreamSetObjectMapper;
import com.automq.rocketmq.metadata.mapper.SequenceMapper;
import com.automq.rocketmq.metadata.mapper.StreamMapper;
import com.automq.rocketmq.metadata.service.cache.S3ObjectCache;
import com.automq.rocketmq.metadata.service.cache.S3StreamObjectCache;
import com.automq.rocketmq.metadata.service.cache.S3StreamSetObjectCache;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import com.google.protobuf.util.JsonFormat;
import com.sun.management.HotSpotDiagnosticMXBean;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultS3MetadataService implements S3MetadataService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultS3MetadataService.class);

    private final ControllerConfig nodeConfig;

    private final SqlSessionFactory sessionFactory;

    private final ExecutorService asyncExecutorService;

    private final S3StreamObjectCache s3StreamObjectCache;

    private final S3ObjectCache s3ObjectCache;

    private final S3StreamSetObjectCache s3StreamSetObjectCache;

    public DefaultS3MetadataService(ControllerConfig nodeConfig, SqlSessionFactory sessionFactory,
        ExecutorService asyncExecutorService) {
        this.nodeConfig = nodeConfig;
        this.sessionFactory = sessionFactory;
        this.asyncExecutorService = asyncExecutorService;
        this.s3StreamObjectCache = new S3StreamObjectCache();
        this.s3ObjectCache = new S3ObjectCache(sessionFactory);
        this.s3StreamSetObjectCache = new S3StreamSetObjectCache(sessionFactory);
    }

    public void start() {
        this.s3StreamSetObjectCache.load(nodeConfig.nodeId());
    }

    public S3ObjectCache getS3ObjectCache() {
        return s3ObjectCache;
    }

    public S3StreamSetObjectCache getS3StreamSetObjectCache() {
        return s3StreamSetObjectCache;
    }

    public CompletableFuture<Long> prepareS3Objects(int count, int ttlInMinutes) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        try (SqlSession session = sessionFactory.openSession()) {
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
        } catch (Exception e) {
            LOGGER.error("PrepareS3Objects failed", e);
            ControllerException ex = new ControllerException(Code.INTERNAL_VALUE, "PrepareS3Objects failed" + e.getMessage());
            future.completeExceptionally(ex);
        }
        return future;
    }

    private void dumpHeap() {
        try {
            HotSpotDiagnosticMXBean mxBean = ManagementFactory.newPlatformMXBeanProxy(ManagementFactory.getPlatformMBeanServer(),
                "com.sun.management:type=HotSpotDiagnostic", HotSpotDiagnosticMXBean.class);
            String userHome = System.getProperty("user.home");
            mxBean.dumpHeap(userHome + File.separator + "heap.hprof", true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public CompletableFuture<Void> commitStreamSetObject(S3StreamSetObject streamSetObject,
        List<S3StreamObject> streamObjects, List<Long> compactedObjects) {
        if (Objects.isNull(streamSetObject)) {
            LOGGER.error("S3StreamSetObject is unexpectedly null");
            ControllerException e = new ControllerException(Code.INTERNAL_VALUE, "S3StreamSetObject is unexpectedly null");
            return CompletableFuture.failedFuture(e);
        }

        LOGGER.info("commitStreamSetObject with StreamSetObject=[{}], streamObjects=[{}], compactedObjects={}",
            TextFormat.shortDebugString(streamSetObject),
            streamObjects.stream()
                .map(TextFormat::shortDebugString)
                .collect(Collectors.joining()), compactedObjects
        );

        // Debug
        for (S3StreamObject item : streamObjects) {
            if (item.getStreamId() <= 0) {
                LOGGER.error("Yuck, S3StreamObject is having invalid stream-id: {}",
                    TextFormat.printer().printToString(item));
                if (nodeConfig.dumpHeapOnError()) {
                    dumpHeap();
                }
            }
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        try (SqlSession session = sessionFactory.openSession()) {
            S3StreamSetObjectMapper s3StreamSetObjectMapper = session.getMapper(S3StreamSetObjectMapper.class);
            S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);

            int brokerId = streamSetObject.getBrokerId();
            long objectId = streamSetObject.getObjectId();

            Map<Long, List<Pair<Long, Long>>> streamSegments = new HashMap<>();
            for (S3StreamObject item : streamObjects) {
                if (!streamSegments.containsKey(item.getStreamId())) {
                    streamSegments.put(item.getStreamId(), new ArrayList<>());
                }
                streamSegments.get(item.getStreamId()).add(new ImmutablePair<>(item.getStartOffset(), item.getEndOffset()));
            }

            streamSetObject.getSubStreams().getSubStreamsMap()
                .forEach((key, value) -> {
                    if (!streamSegments.containsKey(key)) {
                        streamSegments.put(key, new ArrayList<>());
                    }
                    assert key == value.getStreamId();
                    streamSegments.get(key).add(new ImmutablePair<>(value.getStartOffset(), value.getEndOffset()));
                });

            // reduce and verify segment continuity
            Map<Long, Pair<Long, Long>> reduced = new HashMap<>();
            streamSegments.forEach((streamId, list) -> {
                list.sort(Comparator.comparingLong(Pair::getLeft));
                long start = list.get(0).getLeft();
                long current = start;
                for (Pair<Long, Long> p : list) {
                    if (p.getLeft() != current) {
                        LOGGER.warn("Trying to commit an unexpected disjoint stream ranges: {}", list);
                    }
                    current = p.getRight();
                }
                reduced.put(streamId, new ImmutablePair<>(start, current));
            });

            extendRange(session, reduced);

            // commit S3 object
            if (objectId != S3Constants.NOOP_OBJECT_ID && !commitObject(objectId, StreamConstants.NOOP_STREAM_ID, streamSetObject.getObjectSize(), session)) {
                ControllerException e = new ControllerException(Code.ILLEGAL_STATE_VALUE,
                    String.format("S3StreamSetObject[object-id=%d] is not ready for commit", streamSetObject.getObjectId()));
                future.completeExceptionally(e);
                return future;
            }

            long dataTs = System.currentTimeMillis();
            long sequenceId = objectId;
            if (!Objects.isNull(compactedObjects) && !compactedObjects.isEmpty()) {
                List<com.automq.rocketmq.metadata.dao.S3StreamSetObject> s3StreamSetObjects = compactedObjects.stream()
                    .map(id -> {
                        // mark destroy compacted object
                        S3Object object = s3ObjectMapper.getById(id);
                        object.setState(S3ObjectState.BOS_WILL_DELETE);
                        object.setMarkedForDeletionTimestamp(new Date());
                        s3ObjectMapper.markToDelete(object.getId(), new Date());

                        return s3StreamSetObjectMapper.getByObjectId(id);
                    })
                    .toList();

                if (!s3StreamSetObjects.isEmpty()) {
                    // update dataTs to the min compacted object's dataTs
                    dataTs = s3StreamSetObjects.stream()
                        .map(com.automq.rocketmq.metadata.dao.S3StreamSetObject::getBaseDataTimestamp)
                        .map(Date::getTime)
                        .min(Long::compareTo).get();
                    // update sequenceId to the min compacted object's sequenceId
                    sequenceId = s3StreamSetObjects.stream().mapToLong(com.automq.rocketmq.metadata.dao.S3StreamSetObject::getSequenceId).min().getAsLong();
                }
            }

            Map<Long, List<com.automq.rocketmq.metadata.dao.S3StreamObject>> toCache =
                new HashMap<>();

            // commit stream objects;
            if (!streamObjects.isEmpty()) {
                for (apache.rocketmq.controller.v1.S3StreamObject s3StreamObject : streamObjects) {
                    long oId = s3StreamObject.getObjectId();
                    long objectSize = s3StreamObject.getObjectSize();
                    long streamId = s3StreamObject.getStreamId();
                    if (!commitObject(oId, streamId, objectSize, session)) {
                        String msg = String.format("S3StreamObject[object-id=%d] is not ready to commit", oId);
                        ControllerException e = new ControllerException(Code.ILLEGAL_STATE_VALUE, msg);
                        future.completeExceptionally(e);
                        return future;
                    }
                }
                // create stream object records
                streamObjects.forEach(s3StreamObject -> {
                    com.automq.rocketmq.metadata.dao.S3StreamObject object =
                        new com.automq.rocketmq.metadata.dao.S3StreamObject();
                    object.setStreamId(s3StreamObject.getStreamId());
                    object.setObjectId(s3StreamObject.getObjectId());
                    object.setCommittedTimestamp(new Date());
                    object.setStartOffset(s3StreamObject.getStartOffset());
                    object.setBaseDataTimestamp(new Date());
                    object.setEndOffset(s3StreamObject.getEndOffset());
                    object.setObjectSize(s3StreamObject.getObjectSize());
                    s3StreamObjectMapper.commit(object);
                    toCache.computeIfAbsent(object.getStreamId(), streamId -> new ArrayList<>()).add(object);
                });
            }

            // generate compacted objects' remove record ...
            if (!Objects.isNull(compactedObjects) && !compactedObjects.isEmpty()) {
                compactedObjects.forEach(id -> s3StreamSetObjectMapper.delete(id, null, null));
            }

            // update broker's StreamSet object
            if (objectId != S3Constants.NOOP_OBJECT_ID) {
                // generate broker's StreamSet object record
                com.automq.rocketmq.metadata.dao.S3StreamSetObject s3StreamSetObject = new com.automq.rocketmq.metadata.dao.S3StreamSetObject();
                s3StreamSetObject.setObjectId(objectId);
                s3StreamSetObject.setObjectSize(streamSetObject.getObjectSize());
                s3StreamSetObject.setBaseDataTimestamp(new Date(dataTs));
                s3StreamSetObject.setCommittedTimestamp(new Date());
                s3StreamSetObject.setNodeId(brokerId);
                s3StreamSetObject.setSequenceId(sequenceId);
                String subStreams = JsonFormat.printer().print(streamSetObject.getSubStreams());
                s3StreamSetObject.setSubStreams(subStreams);
                s3StreamSetObjectMapper.create(s3StreamSetObject);

                // Cache StreamSet object
                s3StreamSetObjectCache.onCommit(streamSetObject.toBuilder()
                    .setBaseDataTimestamp(s3StreamSetObject.getBaseDataTimestamp().getTime())
                    .setCommittedTimestamp(s3StreamSetObject.getCommittedTimestamp().getTime())
                    .setSequenceId(sequenceId)
                    .build());
            }
            session.commit();

            // Update Cache
            for (Map.Entry<Long, List<com.automq.rocketmq.metadata.dao.S3StreamObject>> entry
                : toCache.entrySet()) {
                s3StreamObjectCache.cache(entry.getKey(), entry.getValue());
            }
            s3StreamSetObjectCache.onCompact(compactedObjects);
            LOGGER.info("broker[broke-id={}] commit StreamSet object[object-id={}] success, compacted objects[{}], stream objects[{}]",
                brokerId, streamSetObject.getObjectId(), compactedObjects, streamObjects);
            future.complete(null);
        } catch (Exception e) {
            LOGGER.error("CommitStreamSetObject failed", e);
            ControllerException ex = new ControllerException(Code.INTERNAL_VALUE, "CommitStreamSetObject failed" + e.getMessage());
            future.completeExceptionally(ex);
        }
        return future;
    }

    public CompletableFuture<Void> commitStreamObject(apache.rocketmq.controller.v1.S3StreamObject streamObject,
        List<Long> compactedObjects) {
        LOGGER.info("commitStreamObject with streamObject: {}, compactedObjects: {}", TextFormat.shortDebugString(streamObject),
            compactedObjects);

        CompletableFuture<Void> future = new CompletableFuture<>();
        try (SqlSession session = sessionFactory.openSession()) {
            if (streamObject.getObjectId() == S3Constants.NOOP_OBJECT_ID) {
                LOGGER.error("S3StreamObject[object-id={}] is null or objectId is unavailable", streamObject.getObjectId());
                String msg = String.format("S3StreamObject[object-id=%d] is null or objectId is unavailable",
                    streamObject.getObjectId());
                ControllerException e = new ControllerException(Code.NOT_FOUND_VALUE, msg);
                future.completeExceptionally(e);
                return future;
            }

            long committedTs = System.currentTimeMillis();
            S3ObjectMapper s3ObjectMapper = session.getMapper(S3ObjectMapper.class);
            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);

            // commit object
            if (!commitObject(streamObject.getObjectId(), streamObject.getStreamId(), streamObject.getObjectSize(), session)) {
                String msg = String.format("S3StreamObject[object-id=%d] is not ready for commit",
                    streamObject.getObjectId());
                ControllerException e = new ControllerException(Code.ILLEGAL_STATE_VALUE, msg);
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
                        com.automq.rocketmq.metadata.dao.S3StreamObject s3StreamObject =
                            s3StreamObjectMapper.getByObjectId(id);
                        return s3StreamObject.getBaseDataTimestamp().getTime();
                    })
                    .min(Long::compareTo).get();
            }

            List<com.automq.rocketmq.metadata.dao.S3StreamObject> toCache = new ArrayList<>();

            // create a new S3StreamObject to replace committed ones
            if (streamObject.getObjectId() != S3Constants.NOOP_OBJECT_ID) {
                com.automq.rocketmq.metadata.dao.S3StreamObject newS3StreamObj =
                    new com.automq.rocketmq.metadata.dao.S3StreamObject();
                newS3StreamObj.setStreamId(streamObject.getStreamId());
                newS3StreamObj.setObjectId(streamObject.getObjectId());
                newS3StreamObj.setObjectSize(streamObject.getObjectSize());
                newS3StreamObj.setStartOffset(streamObject.getStartOffset());
                newS3StreamObj.setEndOffset(streamObject.getEndOffset());
                newS3StreamObj.setBaseDataTimestamp(new Date(dataTs));
                newS3StreamObj.setCommittedTimestamp(new Date(committedTs));
                s3StreamObjectMapper.create(newS3StreamObj);
                toCache.add(newS3StreamObj);
            }

            // delete the compactedObjects of S3Stream
            if (!Objects.isNull(compactedObjects) && !compactedObjects.isEmpty()) {
                compactedObjects.forEach(id -> s3StreamObjectMapper.delete(null, null, id));
            }
            session.commit();

            // Update Cache
            s3StreamObjectCache.cache(streamObject.getStreamId(), toCache);
            s3StreamObjectCache.onCompact(streamObject.getStreamId(), compactedObjects);

            LOGGER.info("S3StreamObject[object-id={}] commit success, compacted objects: {}",
                streamObject.getObjectId(), compactedObjects);
            future.complete(null);
        } catch (Exception e) {
            LOGGER.error("CommitStream failed", e);
            ControllerException ex = new ControllerException(Code.INTERNAL_VALUE, "CommitStream failed" + e.getMessage());
            future.completeExceptionally(ex);
        }
        return future;
    }

    public CompletableFuture<List<S3StreamSetObject>> listStreamSetObjects() {
        CompletableFuture<List<S3StreamSetObject>> future = new CompletableFuture<>();
        try (SqlSession session = sessionFactory.openSession()) {
            S3StreamSetObjectMapper s3StreamSetObjectMapper = session.getMapper(S3StreamSetObjectMapper.class);
            List<S3StreamSetObject> streamSetObjects = s3StreamSetObjectMapper.list(nodeConfig.nodeId(), null).stream()
                .map(s3StreamSetObject -> {
                    try {
                        return Helper.buildS3StreamSetObject(s3StreamSetObject, Helper.decode(s3StreamSetObject.getSubStreams()));
                    } catch (InvalidProtocolBufferException e) {
                        LOGGER.error("Failed to deserialize SubStreams", e);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .toList();
            future.complete(streamSetObjects);
        }
        return future;
    }

    public CompletableFuture<List<S3StreamSetObject>> listStreamSetObjects(long streamId, long startOffset,
        long endOffset, int limit) {
        CompletableFuture<List<S3StreamSetObject>> future = new CompletableFuture<>();
        try (SqlSession session = sessionFactory.openSession()) {
            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);

            List<Integer> nodes = rangeMapper.listByStreamId(streamId)
                .stream()
                .filter(range -> range.getEndOffset() > startOffset && range.getStartOffset() < endOffset)
                .mapToInt(Range::getNodeId)
                .distinct()
                .boxed()
                .toList();

            S3StreamSetObjectMapper s3StreamSetObjectMapper = session.getMapper(S3StreamSetObjectMapper.class);
            List<S3StreamSetObject> finalStreamSetObjects = new ArrayList<>();
            for (int nodeId : nodes) {
                List<com.automq.rocketmq.metadata.dao.S3StreamSetObject> streamSetObjects = s3StreamSetObjectMapper.list(nodeId, null);
                streamSetObjects.stream()
                    .map(s3StreamSetObject -> {
                        try {
                            Map<Long, SubStream> subStreams = Helper.decode(s3StreamSetObject.getSubStreams()).getSubStreamsMap();
                            Map<Long, SubStream> streamsRecords = new HashMap<>();
                            if (subStreams.containsKey(streamId)) {
                                SubStream subStream = subStreams.get(streamId);
                                if (subStream.getStartOffset() <= endOffset && subStream.getEndOffset() > startOffset) {
                                    streamsRecords.put(streamId, subStream);
                                }
                            }
                            if (!streamsRecords.isEmpty()) {
                                return Helper.buildS3StreamSetObject(s3StreamSetObject, SubStreams.newBuilder()
                                    .putAllSubStreams(streamsRecords)
                                    .build());
                            }
                        } catch (InvalidProtocolBufferException e) {
                            LOGGER.error("Failed to deserialize SubStreams", e);
                        }
                        return null;
                    })
                    .filter(Objects::nonNull)
                    .forEach(finalStreamSetObjects::add);
            }

            // Sort by start-offset of the given stream
            finalStreamSetObjects.sort((l, r) -> {
                long lhs = l.getSubStreams().getSubStreamsMap().get(streamId).getStartOffset();
                long rhs = r.getSubStreams().getSubStreamsMap().get(streamId).getStartOffset();
                return Long.compare(lhs, rhs);
            });

            future.complete(finalStreamSetObjects.stream().limit(limit).toList());
        }
        return future;
    }

    public CompletableFuture<List<com.automq.rocketmq.metadata.dao.S3StreamObject>> listStreamObjects0(
        long streamId, long startOffset, long endOffset, int limit) {
        boolean skipCache = false;
        // Serve with cache
        if (s3StreamObjectCache.streamExclusive(streamId)) {
            List<com.automq.rocketmq.metadata.dao.S3StreamObject> list =
                s3StreamObjectCache.listStreamObjects(streamId, startOffset, endOffset, limit);
            if (!list.isEmpty()) {
                return CompletableFuture.completedFuture(list.stream().toList());
            }
            skipCache = true;
        }

        CompletableFuture<List<com.automq.rocketmq.metadata.dao.S3StreamObject>> future =
            new CompletableFuture<>();
        try (SqlSession session = sessionFactory.openSession()) {
            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            S3StreamSetObjectMapper s3StreamSetObjectMapper = session.getMapper(S3StreamSetObjectMapper.class);
            if (!skipCache && s3StreamSetObjectMapper.streamExclusive(nodeConfig.nodeId(), streamId)) {
                s3StreamObjectCache.makeStreamExclusive(streamId);
                List<com.automq.rocketmq.metadata.dao.S3StreamObject> list =
                    s3StreamObjectMapper.listByStreamId(streamId);
                s3StreamObjectCache.initStream(streamId, list);
                return listStreamObjects0(streamId, startOffset, endOffset, limit);
            }
            Long endOffsetObject = endOffset == -1 ? null : endOffset;
            List<com.automq.rocketmq.metadata.dao.S3StreamObject> streamObjects = s3StreamObjectMapper
                .list(null, streamId, startOffset, endOffsetObject, limit);
            future.complete(streamObjects);
        }
        return future;
    }

    public CompletableFuture<List<S3StreamObject>> listStreamObjects(long streamId, long startOffset, long endOffset,
        int limit) {
        return listStreamObjects0(streamId, startOffset, endOffset, limit)
            .thenApply(list -> list.stream().map(this::buildS3StreamObject).toList());
    }

    private S3StreamObject buildS3StreamObject(
        com.automq.rocketmq.metadata.dao.S3StreamObject originalObject) {
        return S3StreamObject.newBuilder()
            .setStreamId(originalObject.getStreamId())
            .setObjectSize(originalObject.getObjectSize())
            .setObjectId(originalObject.getObjectId())
            .setStartOffset(originalObject.getStartOffset())
            .setEndOffset(originalObject.getEndOffset())
            .setBaseDataTimestamp(originalObject.getBaseDataTimestamp().getTime())
            .setCommittedTimestamp(originalObject.getCommittedTimestamp() != null ?
                originalObject.getCommittedTimestamp().getTime() : S3Constants.NOOP_OBJECT_COMMIT_TIMESTAMP)
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

        Date commitDate = new Date();
        if (s3Object.getExpiredTimestamp().getTime() < commitDate.getTime()) {
            LOGGER.error("object[object-id={}] is expired", objectId);
            return false;
        }

        s3Object.setCommittedTimestamp(commitDate);
        s3Object.setStreamId(streamId);
        s3Object.setObjectSize(objectSize);
        s3Object.setState(S3ObjectState.BOS_COMMITTED);
        boolean ok = s3ObjectMapper.commit(s3Object) == 1;
        s3ObjectCache.onObjectAdd(List.of(s3Object));
        return ok;
    }

    private void extendRange(SqlSession session, Map<Long, Pair<Long, Long>> segments) {
        if (segments.isEmpty()) {
            return;
        }
        StreamMapper streamMapper = session.getMapper(StreamMapper.class);
        RangeMapper rangeMapper = session.getMapper(RangeMapper.class);

        for (Map.Entry<Long, Pair<Long, Long>> entry : segments.entrySet()) {
            long streamId = entry.getKey();
            Pair<Long, Long> segment = entry.getValue();
            Stream stream = streamMapper.getByStreamId(streamId);
            if (null == stream) {
                if (nodeConfig.dumpHeapOnError()) {
                    dumpHeap();
                }
                continue;
            }

            if (stream.getState() != StreamState.OPEN) {
                LOGGER.warn("Stream[stream-id={}] state is not OPEN", streamId);
            }

            Range range = rangeMapper.get(stream.getRangeId(), streamId, null);
            if (Objects.isNull(range)) {
                // should not happen
                LOGGER.error("Stream[stream-id={}]'s current range[range-id={}] not exist when stream has been created",
                    streamId, stream.getRangeId());
                continue;
            }

            LOGGER.info("Extend stream range[stream-id={}, range-id={}] with segment [{}, {})",
                streamId, range.getRangeId(), segment.getLeft(), segment.getRight());
            if (segment.getRight() > range.getEndOffset()) {
                range.setEndOffset(segment.getRight());
                rangeMapper.update(range);
            }
        }
    }

    public CompletableFuture<Pair<List<S3StreamObject>, List<S3StreamSetObject>>> listObjects(
        long streamId, long startOffset, long endOffset, int limit) {
        return CompletableFuture.supplyAsync(() -> {
            try (SqlSession session = sessionFactory.openSession()) {
                S3StreamSetObjectMapper s3StreamSetObjectMapper = session.getMapper(S3StreamSetObjectMapper.class);

                List<S3StreamObject> s3StreamObjects =
                    listStreamObjects(streamId, startOffset, endOffset, limit).join();

                List<S3StreamSetObject> streamSetObjects = new ArrayList<>();
                s3StreamSetObjectMapper.list(null, null)
                    .stream()
                    .map(s3StreamSetObject -> {
                        try {
                            Map<Long, SubStream> subStreams = Helper.decode(s3StreamSetObject.getSubStreams()).getSubStreamsMap();
                            Map<Long, SubStream> streamsRecords = new HashMap<>();
                            subStreams.entrySet().stream()
                                .filter(entry -> !Objects.isNull(entry) && entry.getKey().equals(streamId))
                                .filter(entry -> entry.getValue().getEndOffset() > startOffset && (entry.getValue().getStartOffset() <= endOffset || endOffset == -1))
                                .forEach(entry -> streamsRecords.put(entry.getKey(), entry.getValue()));
                            return streamsRecords.isEmpty() ? null : Helper.buildS3StreamSetObject(s3StreamSetObject,
                                SubStreams.newBuilder().putAllSubStreams(streamsRecords).build());
                        } catch (InvalidProtocolBufferException e) {
                            LOGGER.error("Failed to deserialize SubStreams", e);
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .limit(limit)
                    .forEach(streamSetObjects::add);

                if (!streamSetObjects.isEmpty()) {
                    streamSetObjects.sort((l, r) -> {
                        long lhs = l.getSubStreams().getSubStreamsMap().get(streamId).getStartOffset();
                        long rhs = r.getSubStreams().getSubStreamsMap().get(streamId).getStartOffset();
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
                        streamSetObjects.stream()
                            .map(s3StreamSetObject -> new long[] {
                                s3StreamSetObject.getObjectId(),
                                s3StreamSetObject.getSubStreams().getSubStreamsMap().get(streamId).getStartOffset(),
                                s3StreamSetObject.getSubStreams().getSubStreamsMap().get(streamId).getEndOffset()
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

                List<S3StreamSetObject> limitedStreamSetObjectList = streamSetObjects.stream()
                    .filter(s3StreamSetObject -> objectIds.contains(s3StreamSetObject.getObjectId()))
                    .toList();

                return new ImmutablePair<>(limitedStreamObjects, limitedStreamSetObjectList);
            }
        }, asyncExecutorService);
    }

    public CompletableFuture<Void> trimStream(long streamId, long streamEpoch, long newStartOffset) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try (SqlSession session = sessionFactory.openSession()) {
            StreamMapper streamMapper = session.getMapper(StreamMapper.class);
            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);
            S3StreamObjectMapper s3StreamObjectMapper = session.getMapper(S3StreamObjectMapper.class);
            S3StreamSetObjectMapper s3StreamSetObjectMapper = session.getMapper(S3StreamSetObjectMapper.class);
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
                future.completeExceptionally(new ControllerException(Code.ILLEGAL_STATE_VALUE, "Stream is closed"));
                return future;
            }
            if (stream.getStartOffset() > newStartOffset) {
                LOGGER.warn("Stream[{}]‘s start offset {} is larger than request new start offset {}",
                    streamId, stream.getStartOffset(), newStartOffset);
                future.complete(null);
                return future;
            }
            if (stream.getStartOffset() == newStartOffset) {
                // regard it as redundant trim operation, just return success
                future.complete(null);
                return future;
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
                    s3ObjectCache.onObjectDelete(s3Object.getStreamId(), List.of(s3Object.getId()));
                }
            });

            // remove StreamSet object or remove sub-stream range in StreamSet object
            s3StreamSetObjectMapper.list(stream.getDstNodeId(), null).stream()
                .map(s3StreamSetObject -> {
                    try {
                        return Helper.buildS3StreamSetObject(s3StreamSetObject, Helper.decode(s3StreamSetObject.getSubStreams()));
                    } catch (InvalidProtocolBufferException e) {
                        LOGGER.error("Failed to decode");
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .filter(s3StreamSetObject -> s3StreamSetObject.getSubStreams().getSubStreamsMap().containsKey(streamId))
                .filter(s3StreamSetObject -> s3StreamSetObject.getSubStreams().getSubStreamsMap().get(streamId).getEndOffset() <= newStartOffset)
                .forEach(s3StreamSetObject -> {
                    if (s3StreamSetObject.getSubStreams().getSubStreamsMap().size() == 1) {
                        // only this range, but we will remove this range, so now we can remove this StreamSet object
                        S3Object s3Object = s3ObjectMapper.getById(s3StreamSetObject.getObjectId());
                        s3Object.setMarkedForDeletionTimestamp(new Date());
                        s3ObjectMapper.markToDelete(s3Object.getId(), new Date());
                    }

                    // remove offset range about sub-stream ...
                });
            session.commit();

            // Update cache
            s3StreamObjectCache.onTrim(streamId, newStartOffset);

            LOGGER.info("Node[node-id={}] trim stream [stream-id={}] with epoch={} and newStartOffset={}",
                nodeConfig.nodeId(), streamId, streamEpoch, newStartOffset);
            future.complete(null);
        } catch (Exception e) {
            LOGGER.error("TrimStream failed", e);
            ControllerException ex = new ControllerException(Code.INTERNAL_VALUE, "TrimStream failed" + e.getMessage());
            future.completeExceptionally(ex);
        }
        return future;
    }

    @Override
    public long streamDataSize(long streamId) {
        return s3StreamSetObjectCache.streamDataSize(streamId) + s3ObjectCache.streamDataSize(streamId);
    }

    @Override
    public long streamStartTime(long streamId) {
        return Long.min(s3StreamObjectCache.streamStartTime(streamId),
            Long.min(s3ObjectCache.streamStartTime(streamId), s3StreamSetObjectCache.streamStartTime(streamId)));
    }

    @Override
    public void onStreamOpen(long streamId) {
        s3ObjectCache.onStreamOpen(streamId);
    }

    @Override
    public void onStreamClose(long streamId) {
        s3ObjectCache.onStreamClose(streamId);
    }

    @Override
    public void close() throws IOException {

    }
}
