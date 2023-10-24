/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.automq.stream.s3.compact;

import com.automq.stream.s3.Config;
import com.automq.stream.s3.S3ObjectLogger;
import com.automq.stream.s3.compact.objects.CompactedObject;
import com.automq.stream.s3.compact.objects.CompactionType;
import com.automq.stream.s3.compact.objects.StreamDataBlock;
import com.automq.stream.s3.compact.operator.DataBlockReader;
import com.automq.stream.s3.compact.operator.DataBlockWriter;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.StreamMetadata;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.automq.stream.s3.objects.CommitWALObjectRequest;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.objects.ObjectStreamRange;
import com.automq.stream.s3.objects.StreamObject;
import com.automq.stream.s3.operator.DefaultS3Operator;
import com.automq.stream.s3.operator.S3Operator;
import com.automq.stream.s3.streams.StreamManager;
import com.automq.stream.utils.LogContext;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

public class CompactionManager {
    private final Logger logger;
    private final Logger s3ObjectLogger;
    private final ObjectManager objectManager;
    private final StreamManager streamManager;
    private final S3Operator s3Operator;
    private final CompactionAnalyzer compactionAnalyzer;
    private final ScheduledExecutorService scheduledExecutorService;
    private final ExecutorService compactThreadPool;
    private final ExecutorService forceSplitThreadPool;
    private final CompactionUploader uploader;
    private final Config kafkaConfig;
    private final int maxObjectNumToCompact;
    private final int compactionInterval;
    private final int forceSplitObjectPeriod;
    private final int maxStreamNumPerWAL;
    private final int maxStreamObjectNumPerCommit;
    private final boolean s3ObjectLogEnable;
    private final TokenBucketThrottle networkInThrottle;

    public CompactionManager(Config config, ObjectManager objectManager, StreamManager streamManager) {
        this(config, objectManager, streamManager, new DefaultS3Operator(config.s3Endpoint(), config.s3Region(),
            config.s3Bucket(), config.s3ForcePathStyle(), config.s3AccessKey(), config.s3SecretKey()));
    }

    public CompactionManager(Config config, ObjectManager objectManager, StreamManager streamManager, S3Operator s3Operator) {
        String logPrefix = String.format("[CompactionManager id=%d] ", config.brokerId());
        this.logger = new LogContext(logPrefix).logger(CompactionManager.class);
        this.s3ObjectLogger = S3ObjectLogger.logger(logPrefix);
        this.kafkaConfig = config;
        this.objectManager = objectManager;
        this.streamManager = streamManager;
        this.s3Operator = s3Operator;
        this.compactionInterval = config.s3ObjectCompactionInterval();
        this.forceSplitObjectPeriod = config.s3ObjectCompactionForceSplitPeriod();
        this.maxObjectNumToCompact = config.s3ObjectCompactionMaxObjectNum();
        this.s3ObjectLogEnable = config.s3ObjectLogEnable();
        this.networkInThrottle = new TokenBucketThrottle(config.s3ObjectCompactionNWInBandwidth());
        this.uploader = new CompactionUploader(objectManager, s3Operator, config);
        long compactionCacheSize = config.s3ObjectCompactionCacheSize();
        long streamSplitSize = config.s3ObjectCompactionStreamSplitSize();
        maxStreamNumPerWAL = config.s3ObjectMaxStreamNumPerWAL();
        maxStreamObjectNumPerCommit = config.s3ObjectMaxStreamObjectNumPerCommit();
        this.compactionAnalyzer = new CompactionAnalyzer(compactionCacheSize, streamSplitSize, maxStreamNumPerWAL,
                maxStreamObjectNumPerCommit, new LogContext(String.format("[CompactionAnalyzer id=%d] ", config.brokerId())));
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("schedule-compact-executor"));
        this.compactThreadPool = Executors.newFixedThreadPool(1, new DefaultThreadFactory("object-compaction-manager"));
        this.forceSplitThreadPool = Executors.newFixedThreadPool(1, new DefaultThreadFactory("force-split-executor"));
        this.logger.info("Compaction manager initialized with config: compactionInterval: {} min, compactionCacheSize: {} bytes, " +
                        "streamSplitSize: {} bytes, forceSplitObjectPeriod: {} min, maxObjectNumToCompact: {}, maxStreamNumInWAL: {}, maxStreamObjectNum: {}",
                compactionInterval, compactionCacheSize, streamSplitSize, forceSplitObjectPeriod, maxObjectNumToCompact, maxStreamNumPerWAL, maxStreamObjectNumPerCommit);
    }

    public void start() {
        this.scheduledExecutorService.scheduleWithFixedDelay(() -> {
            try {
                logger.info("Compaction started");
                long start = System.currentTimeMillis();
                this.compact()
                        .thenAccept(result -> logger.info("Compaction complete, total cost {} ms, result {}",
                                System.currentTimeMillis() - start, result))
                        .exceptionally(ex -> {
                            logger.error("Compaction failed, cost {} ms, ", System.currentTimeMillis() - start, ex);
                            return null;
                        })
                        .join();
            } catch (Exception ex) {
                logger.error("Error while compacting objects ", ex);
            }
        }, 1, this.compactionInterval, TimeUnit.MINUTES);
    }

    public void shutdown() {
        this.scheduledExecutorService.shutdown();
        this.compactThreadPool.shutdown();
        this.forceSplitThreadPool.shutdown();
        this.networkInThrottle.stop();
        this.uploader.stop();
    }

    private CompletableFuture<CompactResult> compact() {
        return this.objectManager.getServerObjects().thenComposeAsync(objectMetadataList -> {
            List<Long> streamIds = objectMetadataList.stream().flatMap(e -> e.getOffsetRanges().stream())
                    .map(StreamOffsetRange::getStreamId).distinct().toList();
            return this.streamManager.getStreams(streamIds).thenApplyAsync(streamMetadataList -> {
                List<S3ObjectMetadata> s3ObjectMetadataList = objectMetadataList;
                if (s3ObjectMetadataList.isEmpty()) {
                    logger.info("No WAL objects to compact");
                    return CompactResult.SKIPPED;
                }
                // sort by S3 object data time in descending order
                s3ObjectMetadataList.sort((o1, o2) -> Long.compare(o2.dataTimeInMs(), o1.dataTimeInMs()));
                if (maxObjectNumToCompact < s3ObjectMetadataList.size()) {
                    // compact latest S3 objects first when number of objects to compact exceeds maxObjectNumToCompact
                    s3ObjectMetadataList = s3ObjectMetadataList.subList(0, maxObjectNumToCompact);
                }
                return this.compact(streamMetadataList, s3ObjectMetadataList);
            }, compactThreadPool);
        }, compactThreadPool);
    }

    private CompactResult compact(List<StreamMetadata> streamMetadataList, List<S3ObjectMetadata> objectMetadataList) {
        logger.info("Get {} WAL objects from metadata", objectMetadataList.size());
        long start = System.currentTimeMillis();
        while (true) {
            Set<Long> excludedObjectIds = new HashSet<>();
            CommitWALObjectRequest request = buildCompactRequest(streamMetadataList, objectMetadataList, excludedObjectIds);
            if (request == null) {
                return CompactResult.FAILED;
            }
            if (request.getCompactedObjectIds().isEmpty()) {
                logger.info("No need to compact");
                return CompactResult.SKIPPED;
            }
            logger.info("Build compact request complete, {} objects compacted, WAL object id: {}, size: {}, stream object num: {}, time cost: {} ms, start committing objects"
                    , request.getCompactedObjectIds().size(), request.getObjectId(), request.getObjectSize(), request.getStreamObjects().size(), System.currentTimeMillis() - start);
            objectManager.commitWALObject(request).thenApply(resp -> {
                logger.info("Commit compact request succeed, time cost: {} ms", System.currentTimeMillis() - start);
                if (s3ObjectLogEnable) {
                    s3ObjectLogger.trace("[Compact] {}", request);
                }
                return CompactResult.SUCCESS;
            }).join();
            if (request.getObjectId() == -1 && !excludedObjectIds.isEmpty()) {
                // force split objects not complete because of stream object limit, retry force split on excluded objects
                logger.info("Force split not complete, retry on excluded objects {}", excludedObjectIds);
                objectMetadataList = objectMetadataList.stream().filter(e -> excludedObjectIds.contains(e.objectId())).collect(Collectors.toList());
                continue;
            }
            break;
        }
        return CompactResult.SUCCESS;
    }

    private void logCompactionPlans(List<CompactionPlan> compactionPlans, Set<Long> excludedObjectIds) {
        if (compactionPlans.isEmpty()) {
            logger.info("No compaction plans to execute");
            return;
        }
        long streamObjectNum = compactionPlans.stream()
                .mapToLong(p -> p.compactedObjects().stream()
                        .filter(o -> o.type() == CompactionType.SPLIT)
                        .count())
                .sum();
        long walObjectSize = compactionPlans.stream()
                .mapToLong(p -> p.compactedObjects().stream()
                        .filter(o -> o.type() == CompactionType.COMPACT)
                        .mapToLong(CompactedObject::size)
                        .sum())
                .sum();
        logger.info("Compaction plans: expect to generate {} StreamObject, 1 WAL object with size {} in {} iterations, objects excluded: {}",
                streamObjectNum, walObjectSize, compactionPlans.size(), excludedObjectIds);
    }

    public CompletableFuture<Void> forceSplitAll() {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        //TODO: deal with metadata delay
        this.scheduledExecutorService.execute(() -> this.objectManager.getServerObjects().thenAcceptAsync(objectMetadataList -> {
            List<Long> streamIds = objectMetadataList.stream().flatMap(e -> e.getOffsetRanges().stream())
                    .map(StreamOffsetRange::getStreamId).distinct().toList();
            this.streamManager.getStreams(streamIds).thenAcceptAsync(streamMetadataList -> {
                if (objectMetadataList.isEmpty()) {
                    logger.info("No WAL objects to force split");
                    return;
                }
                Set<Long> excludedObjects = new HashSet<>();
                CompletableFuture<Void> forceSplitCf = forceSplitAndCommit(streamMetadataList, objectMetadataList, excludedObjects);
                while (!excludedObjects.isEmpty()) {
                    // try split excluded objects
                    List<S3ObjectMetadata> excludedObjectMetaList = objectMetadataList.stream()
                            .filter(e -> excludedObjects.contains(e.objectId())).collect(Collectors.toList());
                    forceSplitCf = forceSplitCf.thenCompose(vv -> forceSplitAndCommit(streamMetadataList, excludedObjectMetaList, excludedObjects));
                }
                forceSplitCf.whenComplete((vv, ex) -> {
                    if (ex != null) {
                        cf.completeExceptionally(ex);
                    } else {
                        cf.complete(null);
                    }
                });
            }, forceSplitThreadPool);
        }, forceSplitThreadPool).exceptionally(ex -> {
            logger.error("Error while force split all WAL objects ", ex);
            cf.completeExceptionally(ex);
            return null;
        }));

        return cf;
    }

    private CompletableFuture<Void> forceSplitAndCommit(List<StreamMetadata> streams, List<S3ObjectMetadata> objects, Set<Long> excludedObjects) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        Collection<CompletableFuture<StreamObject>> cfList = splitWALObjects(streams, objects, excludedObjects);
        List<StreamObject> streamObjects = cfList.stream().map(e -> {
            try {
                return e.join();
            } catch (Exception ex) {
                logger.error("Error while force split object ", ex);
            }
            return null;
        }).toList();
        if (streamObjects.stream().anyMatch(Objects::isNull)) {
            logger.error("Force split WAL objects failed");
            cf.completeExceptionally(new RuntimeException("Force split WAL objects failed"));
            return cf;
        }
        CommitWALObjectRequest request = new CommitWALObjectRequest();
        streamObjects.forEach(request::addStreamObject);
        request.setCompactedObjectIds(objects.stream().map(S3ObjectMetadata::objectId).collect(Collectors.toList()));
        objectManager.commitWALObject(request).thenAccept(resp -> {
            logger.info("Force split {} WAL objects succeed, produce {} stream objects", objects.size(), streamObjects.size());
            if (s3ObjectLogEnable) {
                s3ObjectLogger.trace("[ForceSplit] {}", request);
            }
            cf.complete(null);
        }).exceptionally(ex -> {
            logger.error("Force split all WAL objects failed", ex);
            cf.completeExceptionally(ex);
            return null;
        });
        return cf;
    }

    /**
     * Split specified WAL objects into stream objects.
     *
     * @param streamMetadataList metadata of opened streams
     * @param objectMetadataList WAL objects to split
     * @param excludedObjects objects that are excluded from split
     * @return List of CompletableFuture of StreamObject
     */
    Collection<CompletableFuture<StreamObject>> splitWALObjects(List<StreamMetadata> streamMetadataList,
                                                                List<S3ObjectMetadata> objectMetadataList, Set<Long> excludedObjects) {
        if (objectMetadataList.isEmpty()) {
            return new ArrayList<>();
        }

        Map<Long, List<StreamDataBlock>> streamDataBlocksMap = CompactionUtils.blockWaitObjectIndices(streamMetadataList, objectMetadataList, s3Operator);
        List<Pair<List<StreamDataBlock>, CompletableFuture<StreamObject>>> groupedDataBlocks = new ArrayList<>();
        int totalStreamObjectNum = 0;
        // set of object ids to be included in split
        Set<Long> includedObjects = new HashSet<>();
        // list of <object_id, list of stream objects>, each stream object is a list of adjacent stream data blocks
        List<Pair<Long, List<List<StreamDataBlock>>>> sortedObjectGroupedStreamDataBlockList = new ArrayList<>();
        for (Map.Entry<Long, List<StreamDataBlock>> entry : streamDataBlocksMap.entrySet()) {
            // group continuous stream data blocks, each group will be written to a stream object
            List<List<StreamDataBlock>> groupedStreamDataBlocks = CompactionUtils.groupStreamDataBlocks(entry.getValue());
            sortedObjectGroupedStreamDataBlockList.add(new ImmutablePair<>(entry.getKey(), groupedStreamDataBlocks));
        }
        // sort WAL object by number of stream objects to be generated, object with less stream objects will be split first
        sortedObjectGroupedStreamDataBlockList.sort(Comparator.comparingInt(e -> e.getRight().size()));
        for (Pair<Long, List<List<StreamDataBlock>>> pair : sortedObjectGroupedStreamDataBlockList) {
            long objectId = pair.getLeft();
            List<List<StreamDataBlock>> groupedStreamDataBlocks = pair.getRight();
            if (totalStreamObjectNum + groupedStreamDataBlocks.size() > maxStreamObjectNumPerCommit) {
                // exceed max stream object number, stop split
                break;
            }
            for (List<StreamDataBlock> streamDataBlocks : groupedStreamDataBlocks) {
                groupedDataBlocks.add(new ImmutablePair<>(streamDataBlocks, new CompletableFuture<>()));
            }
            includedObjects.add(objectId);
            totalStreamObjectNum += groupedStreamDataBlocks.size();
        }
        // add objects that are excluded from split
        excludedObjects.addAll(streamDataBlocksMap.keySet().stream().filter(e -> !includedObjects.contains(e)).collect(Collectors.toSet()));
        logger.info("Force split {} WAL objects, expect to generate {} stream objects, max stream objects {}, objects excluded: {}",
                objectMetadataList.size(), groupedDataBlocks.size(), maxStreamObjectNumPerCommit, excludedObjects);
        if (groupedDataBlocks.isEmpty()) {
            return new ArrayList<>();
        }
        // prepare N stream objects at one time
        objectManager.prepareObject(groupedDataBlocks.size(), TimeUnit.MINUTES.toMillis(60))
                .thenAcceptAsync(objectId -> {
                    for (Pair<List<StreamDataBlock>, CompletableFuture<StreamObject>> pair : groupedDataBlocks) {
                        List<StreamDataBlock> streamDataBlocks = pair.getKey();
                        DataBlockWriter writer = new DataBlockWriter(objectId, s3Operator, kafkaConfig.s3ObjectPartSize());
                        writer.copyWrite(streamDataBlocks);
                        final long objectIdFinal = objectId;
                        writer.close().thenAccept(v -> {
                            StreamObject streamObject = new StreamObject();
                            streamObject.setObjectId(objectIdFinal);
                            streamObject.setStreamId(streamDataBlocks.get(0).getStreamId());
                            streamObject.setStartOffset(streamDataBlocks.get(0).getStartOffset());
                            streamObject.setEndOffset(streamDataBlocks.get(streamDataBlocks.size() - 1).getEndOffset());
                            streamObject.setObjectSize(writer.size());
                            pair.getValue().complete(streamObject);
                        });
                        objectId++;
                    }
                }, forceSplitThreadPool).exceptionally(ex -> {
                    logger.error("Prepare object failed", ex);
                    for (Pair<List<StreamDataBlock>, CompletableFuture<StreamObject>> pair : groupedDataBlocks) {
                        pair.getValue().completeExceptionally(ex);
                    }
                    return null;
                });

        return groupedDataBlocks.stream().map(Pair::getValue).collect(Collectors.toList());
    }

    CommitWALObjectRequest buildCompactRequest(List<StreamMetadata> streamMetadataList, List<S3ObjectMetadata> s3ObjectMetadata, Set<Long> excludedObjectIds) {
        Map<Boolean, List<S3ObjectMetadata>> objectMetadataFilterMap = convertS3Objects(s3ObjectMetadata);
        List<S3ObjectMetadata> objectsToSplit = objectMetadataFilterMap.get(true);
        List<S3ObjectMetadata> objectsToCompact = objectMetadataFilterMap.get(false);
        // force split objects that exists for too long
        logger.info("{} WAL objects to be force split, total split size {}", objectsToSplit.size(),
                objectMetadataFilterMap.get(true).stream().mapToLong(S3ObjectMetadata::objectSize).sum());
        Collection<CompletableFuture<StreamObject>> forceSplitCfs = splitWALObjects(streamMetadataList, objectsToSplit, excludedObjectIds);

        CommitWALObjectRequest request = new CommitWALObjectRequest();
        request.setObjectId(-1L);

        List<CompactionPlan> compactionPlans = new ArrayList<>();
        if (excludedObjectIds.isEmpty()) {
            // compact WAL objects only when compaction limitations are not violated after force split
            try {
                logger.info("{} WAL objects as compact candidates, total compaction size: {}",
                        objectsToCompact.size(), objectsToCompact.stream().mapToLong(S3ObjectMetadata::objectSize).sum());
                Map<Long, List<StreamDataBlock>> streamDataBlockMap = CompactionUtils.blockWaitObjectIndices(streamMetadataList, objectsToCompact, s3Operator);
                long now = System.currentTimeMillis();
                compactionPlans = this.compactionAnalyzer.analyze(streamDataBlockMap, excludedObjectIds);
                logger.info("Analyze compaction plans complete, cost {}ms", System.currentTimeMillis() - now);
                logCompactionPlans(compactionPlans, excludedObjectIds);
                objectsToCompact = objectsToCompact.stream().filter(e -> !excludedObjectIds.contains(e.objectId())).collect(Collectors.toList());
                compactWALObjects(request, compactionPlans, objectsToCompact);
            } catch (Exception e) {
                logger.error("Error while compacting objects ", e);
            }
        }

        forceSplitCfs.stream().map(e -> {
            try {
                return e.join();
            } catch (Exception ex) {
                logger.error("Force split StreamObject failed ", ex);
                return null;
            }
        }).filter(Objects::nonNull).forEach(request::addStreamObject);

        Set<Long> compactedObjectIds = new HashSet<>();
        objectMetadataFilterMap.get(true).forEach(e -> {
            if (!excludedObjectIds.contains(e.objectId())) {
                compactedObjectIds.add(e.objectId());
            }
        });
        compactionPlans.forEach(c -> c.streamDataBlocksMap().values().forEach(v -> v.forEach(b -> compactedObjectIds.add(b.getObjectId()))));
        request.setCompactedObjectIds(new ArrayList<>(compactedObjectIds));

        if (!sanityCheckCompactionResult(streamMetadataList, objectsToSplit, objectsToCompact, request)) {
            logger.error("Sanity check failed, compaction result is illegal");
            return null;
        }

        return request;
    }

    boolean sanityCheckCompactionResult(List<StreamMetadata> streamMetadataList, List<S3ObjectMetadata> objectsToSplit,
                                        List<S3ObjectMetadata> objectsToCompact, CommitWALObjectRequest request) {
        Map<Long, StreamMetadata> streamMetadataMap = streamMetadataList.stream()
                .collect(Collectors.toMap(StreamMetadata::getStreamId, e -> e));
        Map<Long, S3ObjectMetadata> objectMetadataMap = objectsToCompact.stream()
                .collect(Collectors.toMap(S3ObjectMetadata::objectId, e -> e));
        objectMetadataMap.putAll(objectsToSplit.stream()
                .collect(Collectors.toMap(S3ObjectMetadata::objectId, e -> e)));

        List<StreamOffsetRange> compactedStreamOffsetRanges = new ArrayList<>();
        request.getStreamRanges().forEach(o -> compactedStreamOffsetRanges.add(new StreamOffsetRange(o.getStreamId(), o.getStartOffset(), o.getEndOffset())));
        request.getStreamObjects().forEach(o -> compactedStreamOffsetRanges.add(new StreamOffsetRange(o.getStreamId(), o.getStartOffset(), o.getEndOffset())));
        Map<Long, List<StreamOffsetRange>> sortedStreamOffsetRanges = compactedStreamOffsetRanges.stream()
                .collect(Collectors.groupingBy(StreamOffsetRange::getStreamId));
        sortedStreamOffsetRanges.replaceAll((k, v) -> sortAndMerge(v));
        for (long objectId : request.getCompactedObjectIds()) {
            S3ObjectMetadata metadata = objectMetadataMap.get(objectId);
            for (StreamOffsetRange streamOffsetRange : metadata.getOffsetRanges()) {
                if (!streamMetadataMap.containsKey(streamOffsetRange.getStreamId()) ||
                        streamOffsetRange.getEndOffset() <= streamMetadataMap.get(streamOffsetRange.getStreamId()).getStartOffset()) {
                    // skip stream offset range that has been trimmed
                    continue;
                }
                if (!sortedStreamOffsetRanges.containsKey(streamOffsetRange.getStreamId())) {
                    logger.error("Sanity check failed, stream {} is missing after compact", streamOffsetRange.getStreamId());
                    return false;
                }
                boolean contained = false;
                for (StreamOffsetRange compactedStreamOffsetRange : sortedStreamOffsetRanges.get(streamOffsetRange.getStreamId())) {
                    if (streamOffsetRange.getStartOffset() >= compactedStreamOffsetRange.getStartOffset()
                            && streamOffsetRange.getEndOffset() <= compactedStreamOffsetRange.getEndOffset()) {
                        contained = true;
                        break;
                    }
                }
                if (!contained) {
                    logger.error("Sanity check failed, object {} offset range {} is missing after compact", objectId, streamOffsetRange);
                    return false;
                }
            }
        }

        return true;
    }

    private List<StreamOffsetRange> sortAndMerge(List<StreamOffsetRange> streamOffsetRangeList) {
        if (streamOffsetRangeList.size() < 2) {
            return streamOffsetRangeList;
        }
        long streamId = streamOffsetRangeList.get(0).getStreamId();
        Collections.sort(streamOffsetRangeList);
        List<StreamOffsetRange> mergedList = new ArrayList<>();
        long start  = -1L;
        long end = -1L;
        for (int i = 0; i < streamOffsetRangeList.size() - 1; i++) {
            StreamOffsetRange curr = streamOffsetRangeList.get(i);
            StreamOffsetRange next = streamOffsetRangeList.get(i + 1);
            if (start == -1) {
                start = curr.getStartOffset();
                end = curr.getEndOffset();
            }
            if (curr.getEndOffset() < next.getStartOffset()) {
                mergedList.add(new StreamOffsetRange(curr.getStreamId(), start, end));
                start = next.getStartOffset();
            }
            end = next.getEndOffset();
        }
        mergedList.add(new StreamOffsetRange(streamId, start, end));

        return mergedList;
    }

    Map<Boolean, List<S3ObjectMetadata>> convertS3Objects(List<S3ObjectMetadata> s3WALObjectMetadata) {
        return new HashMap<>(s3WALObjectMetadata.stream()
                .collect(Collectors.partitioningBy(e -> (System.currentTimeMillis() - e.dataTimeInMs())
                        >= TimeUnit.MINUTES.toMillis(this.forceSplitObjectPeriod))));
    }

    void compactWALObjects(CommitWALObjectRequest request, List<CompactionPlan> compactionPlans, List<S3ObjectMetadata> s3ObjectMetadata)
            throws IllegalArgumentException {
        if (compactionPlans.isEmpty()) {
            return;
        }
        Map<Long, S3ObjectMetadata> s3ObjectMetadataMap = s3ObjectMetadata.stream()
                .collect(Collectors.toMap(S3ObjectMetadata::objectId, e -> e));
        List<StreamDataBlock> sortedStreamDataBlocks = new ArrayList<>();
        for (int i = 0; i < compactionPlans.size(); i++) {
            // iterate over each compaction plan
            CompactionPlan compactionPlan = compactionPlans.get(i);
            long totalSize = compactionPlan.streamDataBlocksMap().values().stream().flatMap(List::stream)
                    .mapToLong(StreamDataBlock::getBlockSize).sum();
            logger.info("Compaction progress {}/{}, read from {} WALs, total size: {}", i + 1, compactionPlans.size(),
                    compactionPlan.streamDataBlocksMap().size(), totalSize);
            for (Map.Entry<Long, List<StreamDataBlock>> streamDataBlocEntry : compactionPlan.streamDataBlocksMap().entrySet()) {
                S3ObjectMetadata metadata = s3ObjectMetadataMap.get(streamDataBlocEntry.getKey());
                List<StreamDataBlock> streamDataBlocks = streamDataBlocEntry.getValue();
                DataBlockReader reader = new DataBlockReader(metadata, s3Operator);
                reader.readBlocks(streamDataBlocks, networkInThrottle);
            }
            List<CompletableFuture<StreamObject>> streamObjectCFList = new ArrayList<>();
            CompletableFuture<Void> walObjectCF = null;
            for (CompactedObject compactedObject : compactionPlan.compactedObjects()) {
                if (compactedObject.type() == CompactionType.COMPACT) {
                    sortedStreamDataBlocks.addAll(compactedObject.streamDataBlocks());
                    walObjectCF = uploader.chainWriteWALObject(walObjectCF, compactedObject);
                } else {
                    streamObjectCFList.add(uploader.writeStreamObject(compactedObject));
                }
            }

            // wait for all stream objects and wal object part to be uploaded
            try {
                if (walObjectCF != null) {
                    // wait for all writes done
                    walObjectCF.thenAccept(v -> uploader.forceUploadWAL()).join();
                }
                streamObjectCFList.stream().map(CompletableFuture::join).forEach(request::addStreamObject);
            } catch (Exception ex) {
                logger.error("Error while uploading compaction objects", ex);
                uploader.reset();
                throw new IllegalArgumentException("Error while uploading compaction objects", ex);
            }
        }
        List<ObjectStreamRange> objectStreamRanges = CompactionUtils.buildObjectStreamRange(sortedStreamDataBlocks);
        objectStreamRanges.forEach(request::addStreamRange);
        request.setObjectId(uploader.getWALObjectId());
        // set wal object id to be the first object id of compacted objects
        request.setOrderId(s3ObjectMetadata.get(0).objectId());
        request.setObjectSize(uploader.completeWAL());
        uploader.reset();
    }
}
