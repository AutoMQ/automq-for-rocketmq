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

import com.automq.stream.s3.compact.objects.CompactedObject;
import com.automq.stream.s3.compact.objects.CompactedObjectBuilder;
import com.automq.stream.s3.compact.objects.CompactionType;
import com.automq.stream.s3.compact.objects.StreamDataBlock;
import com.automq.stream.utils.LogContext;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class CompactionAnalyzer {
    private final Logger logger;
    private final long compactionCacheSize;
    private final long streamSplitSize;
    private final int maxStreamNumInWAL;
    private final int maxStreamObjectNum;

    public CompactionAnalyzer(long compactionCacheSize, long streamSplitSize, int maxStreamNumInWAL, int maxStreamObjectNum) {
        this(compactionCacheSize, streamSplitSize, maxStreamNumInWAL, maxStreamObjectNum, new LogContext("[CompactionAnalyzer]"));
    }

    public CompactionAnalyzer(long compactionCacheSize, long streamSplitSize,
                              int maxStreamNumInWAL, int maxStreamObjectNum, LogContext logContext) {
        this.logger = logContext.logger(CompactionAnalyzer.class);
        this.compactionCacheSize = compactionCacheSize;
        this.streamSplitSize = streamSplitSize;
        this.maxStreamNumInWAL = maxStreamNumInWAL;
        this.maxStreamObjectNum = maxStreamObjectNum;
    }

    public List<CompactionPlan> analyze(Map<Long, List<StreamDataBlock>> streamDataBlockMap, Set<Long> excludedObjectIds) {
        if (streamDataBlockMap.isEmpty()) {
            return Collections.emptyList();
        }
        streamDataBlockMap = filterBlocksToCompact(streamDataBlockMap);
        this.logger.info("{} WAL objects to compact after filter", streamDataBlockMap.size());
        if (streamDataBlockMap.isEmpty()) {
            return Collections.emptyList();
        }
        try {
            List<CompactedObjectBuilder> compactedObjectBuilders = groupObjectWithLimits(streamDataBlockMap, excludedObjectIds);
            return generatePlanWithCacheLimit(compactedObjectBuilders);
        } catch (Exception e) {
            logger.error("Error while analyzing compaction plan", e);
        }
        return Collections.emptyList();
    }

    List<CompactedObjectBuilder> groupObjectWithLimits(Map<Long, List<StreamDataBlock>> streamDataBlockMap, Set<Long> excludedObjectIds) {
        List<StreamDataBlock> sortedStreamDataBlocks = sortStreamRangePositions(streamDataBlockMap);
        List<CompactedObjectBuilder> compactedObjectBuilders = new ArrayList<>();
        CompactionStats stats = null;
        int streamNumInWAL = -1;
        int streamObjectNum = -1;
        do {
            final Set<Long> objectsToRemove = new HashSet<>();
            if (stats != null) {
                if (streamObjectNum > maxStreamObjectNum) {
                    logger.warn("Stream object num {} exceeds limit {}, try to reduce number of objects to compact", streamObjectNum, maxStreamObjectNum);
                    addObjectsToRemove(CompactionType.SPLIT, compactedObjectBuilders, stats, objectsToRemove);
                } else {
                    logger.warn("Stream number {} exceeds limit {}, try to reduce number of objects to compact", streamNumInWAL, maxStreamNumInWAL);
                    addObjectsToRemove(CompactionType.COMPACT, compactedObjectBuilders, stats, objectsToRemove);
                }
                if (objectsToRemove.isEmpty()) {
                    logger.error("Unable to derive objects to exclude, compaction failed");
                    return new ArrayList<>();
                }
            }
            if (!objectsToRemove.isEmpty()) {
                logger.info("Excluded objects {} for compaction", objectsToRemove);
                excludedObjectIds.addAll(objectsToRemove);
            }
            sortedStreamDataBlocks.removeIf(e -> objectsToRemove.contains(e.getObjectId()));
            objectsToRemove.forEach(streamDataBlockMap::remove);
            streamDataBlockMap = filterBlocksToCompact(streamDataBlockMap);
            if (streamDataBlockMap.isEmpty()) {
                logger.warn("No viable objects to compact after exclusion");
                return new ArrayList<>();
            }
            compactedObjectBuilders = compactObjects(sortedStreamDataBlocks);
            stats = CompactionStats.of(compactedObjectBuilders);
            streamNumInWAL = stats.getStreamRecord().streamNumInWAL();
            streamObjectNum = stats.getStreamRecord().streamObjectNum();
            logger.info("Current stream num in WAL: {}, max: {}, stream object num: {}, max: {}", streamNumInWAL, maxStreamNumInWAL, streamObjectNum, maxStreamObjectNum);
        } while (streamNumInWAL > maxStreamNumInWAL || streamObjectNum > maxStreamObjectNum);

        return compactedObjectBuilders;
    }

    private void addObjectsToRemove(CompactionType compactionType, List<CompactedObjectBuilder> compactedObjectBuilders,
                                    CompactionStats stats, Set<Long> objectsToRemove) {
        List<CompactedObjectBuilder> sortedCompactedObjectIndexList = new ArrayList<>();
        for (CompactedObjectBuilder compactedObjectBuilder : compactedObjectBuilders) {
            if (compactedObjectBuilder.type() == compactionType) {
                sortedCompactedObjectIndexList.add(compactedObjectBuilder);
            }
        }
        if (compactionType == CompactionType.SPLIT) {
            sortedCompactedObjectIndexList.sort(new StreamObjectComparator(stats.getS3ObjectToCompactedObjectNumMap()));
            CompactedObjectBuilder compactedObjectToRemove = sortedCompactedObjectIndexList.get(0);
            objectsToRemove.addAll(compactedObjectToRemove.streamDataBlocks().stream()
                    .map(StreamDataBlock::getObjectId)
                    .collect(Collectors.toSet()));
        } else {
            Map<Long, Set<Long>> streamObjectIdsMap = new HashMap<>();
            Map<Long, Set<Long>> objectStreamIdsMap = new HashMap<>();
            for (CompactedObjectBuilder compactedObjectBuilder : sortedCompactedObjectIndexList) {
                for (StreamDataBlock streamDataBlock : compactedObjectBuilder.streamDataBlocks()) {
                    Set<Long> objectIds = streamObjectIdsMap.computeIfAbsent(streamDataBlock.getStreamId(), k -> new HashSet<>());
                    objectIds.add(streamDataBlock.getObjectId());
                    Set<Long> streamIds = objectStreamIdsMap.computeIfAbsent(streamDataBlock.getObjectId(), k -> new HashSet<>());
                    streamIds.add(streamDataBlock.getStreamId());
                }
            }
            List<Pair<Long, Integer>> sortedStreamObjectStatsList = new ArrayList<>();
            for (Map.Entry<Long, Set<Long>> entry : streamObjectIdsMap.entrySet()) {
                long streamId = entry.getKey();
                Set<Long> objectIds = entry.getValue();
                int objectStreamNum = 0;
                for (long objectId : objectIds) {
                    objectStreamNum += objectStreamIdsMap.get(objectId).size();
                }
                sortedStreamObjectStatsList.add(new ImmutablePair<>(streamId, objectStreamNum));
            }
            sortedStreamObjectStatsList.sort(Comparator.comparingInt(Pair::getRight));
            objectsToRemove.addAll(streamObjectIdsMap.get(sortedStreamObjectStatsList.get(0).getKey()));
        }
    }

    List<CompactionPlan> generatePlanWithCacheLimit(List<CompactedObjectBuilder> compactedObjectBuilders) {
        List<CompactionPlan> compactionPlans = new ArrayList<>();
        List<CompactedObject> compactedObjects = new ArrayList<>();
        CompactedObjectBuilder compactedWALObjectBuilder = null;
        long totalSize = 0L;
        for (int i = 0; i < compactedObjectBuilders.size(); ) {
            CompactedObjectBuilder compactedObjectBuilder = compactedObjectBuilders.get(i);
            if (totalSize + compactedObjectBuilder.totalBlockSize() > compactionCacheSize) {
                if (shouldSplitObject(compactedObjectBuilder)) {
                    // split object to fit into cache
                    int endOffset = 0;
                    long tmpSize = totalSize;
                    for (int j = 0; j < compactedObjectBuilder.streamDataBlocks().size(); j++) {
                        tmpSize += compactedObjectBuilder.streamDataBlocks().get(j).getBlockSize();
                        if (tmpSize > compactionCacheSize) {
                            endOffset = j;
                            break;
                        }
                    }
                    if (endOffset != 0) {
                        CompactedObjectBuilder builder = compactedObjectBuilder.split(0, endOffset);
                        compactedWALObjectBuilder = addOrMergeCompactedObject(builder, compactedObjects, compactedWALObjectBuilder);
                    }
                }
                compactionPlans.add(generateCompactionPlan(compactedObjects, compactedWALObjectBuilder));
                compactedObjects.clear();
                compactedWALObjectBuilder = null;
                totalSize = 0;
            } else {
                // object fits into cache size
                compactedWALObjectBuilder = addOrMergeCompactedObject(compactedObjectBuilder, compactedObjects, compactedWALObjectBuilder);
                totalSize += compactedObjectBuilder.totalBlockSize();
                i++;
            }

        }
        if (!compactedObjects.isEmpty() || compactedWALObjectBuilder != null) {
            compactionPlans.add(generateCompactionPlan(compactedObjects, compactedWALObjectBuilder));
        }
        return compactionPlans;
    }

    private CompactedObjectBuilder addOrMergeCompactedObject(CompactedObjectBuilder compactedObjectBuilder,
                                                             List<CompactedObject> compactedObjects,
                                                             CompactedObjectBuilder compactedWALObjectBuilder) {
        if (compactedObjectBuilder.type() == CompactionType.SPLIT) {
            compactedObjects.add(compactedObjectBuilder.build());
        } else {
            if (compactedWALObjectBuilder == null) {
                compactedWALObjectBuilder = new CompactedObjectBuilder();
            }
            compactedWALObjectBuilder.merge(compactedObjectBuilder);
        }
        return compactedWALObjectBuilder;
    }

    private boolean shouldSplitObject(CompactedObjectBuilder compactedObjectBuilder) {
        //TODO: split object depends on available cache size and current object size
        //TODO: use multipart upload to upload split stream object
        return true;
    }

    private CompactionPlan generateCompactionPlan(List<CompactedObject> compactedObjects, CompactedObjectBuilder compactedWALObject) {
        if (compactedWALObject != null) {
            compactedObjects.add(compactedWALObject.build());
        }
        Map<Long, List<StreamDataBlock>> streamDataBlockMap = new HashMap<>();
        for (CompactedObject compactedObject : compactedObjects) {
            for (StreamDataBlock streamDataBlock : compactedObject.streamDataBlocks()) {
                streamDataBlockMap.computeIfAbsent(streamDataBlock.getObjectId(), k -> new ArrayList<>()).add(streamDataBlock);
            }
        }
        for (List<StreamDataBlock> dataBlocks : streamDataBlockMap.values()) {
            dataBlocks.sort(StreamDataBlock.BLOCK_POSITION_COMPARATOR);
        }

        return new CompactionPlan(new ArrayList<>(compactedObjects), streamDataBlockMap);
    }

    private List<CompactedObjectBuilder> compactObjects(List<StreamDataBlock> streamDataBlocks) {
        List<CompactedObjectBuilder> compactedObjectBuilders = new ArrayList<>();
        CompactedObjectBuilder builder = new CompactedObjectBuilder();
        for (StreamDataBlock streamDataBlock : streamDataBlocks) {
            if (builder.lastStreamId() == -1L) {
                // init state
                builder.addStreamDataBlock(streamDataBlock);
            } else if (builder.lastStreamId() == streamDataBlock.getStreamId()) {
                // data range from same stream
                if (streamDataBlock.getStartOffset() > builder.lastOffset()) {
                    // data range is not continuous, split current object as StreamObject
                    builder = splitObject(builder, compactedObjectBuilders);
                    builder.addStreamDataBlock(streamDataBlock);
                } else if (streamDataBlock.getStartOffset() == builder.lastOffset()) {
                    builder.addStreamDataBlock(streamDataBlock);
                } else {
                    // should not go there
                    logger.error("FATAL ERROR: illegal stream range position, last offset: {}, curr: {}",
                            builder.lastOffset(), streamDataBlock);
                    return new ArrayList<>();
                }
            } else {
                builder = splitAndAddBlock(builder, streamDataBlock, compactedObjectBuilders);
            }
        }
        if (builder.currStreamBlockSize() > streamSplitSize) {
            splitObject(builder, compactedObjectBuilders);
        } else {
            compactedObjectBuilders.add(builder);
        }
        return compactedObjectBuilders;
    }

    Map<Long, List<StreamDataBlock>> filterBlocksToCompact(Map<Long, List<StreamDataBlock>> streamDataBlocksMap) {
        Map<Long, Set<Long>> streamToObjectIds = streamDataBlocksMap.values().stream()
                .flatMap(Collection::stream)
                .collect(Collectors.groupingBy(StreamDataBlock::getStreamId, Collectors.mapping(StreamDataBlock::getObjectId, Collectors.toSet())));
        Set<Long> objectIdsToCompact = streamToObjectIds
                .entrySet().stream()
                .filter(e -> e.getValue().size() > 1)
                .flatMap(e -> e.getValue().stream())
                .collect(Collectors.toSet());
        return streamDataBlocksMap.entrySet().stream()
                .filter(e -> objectIdsToCompact.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private CompactedObjectBuilder splitAndAddBlock(CompactedObjectBuilder builder,
                                                    StreamDataBlock streamDataBlock,
                                                    List<CompactedObjectBuilder> compactedObjectBuilders) {
        if (builder.currStreamBlockSize() > streamSplitSize) {
            builder = splitObject(builder, compactedObjectBuilders);
        }
        builder.addStreamDataBlock(streamDataBlock);
        return builder;
    }

    private CompactedObjectBuilder splitObject(CompactedObjectBuilder builder,
                                               List<CompactedObjectBuilder> compactedObjectBuilders) {
        CompactedObjectBuilder splitBuilder = builder.splitCurrentStream();
        splitBuilder.setType(CompactionType.SPLIT);
        if (builder.totalBlockSize() != 0) {
            compactedObjectBuilders.add(builder);
        }
        compactedObjectBuilders.add(splitBuilder);
        builder = new CompactedObjectBuilder();
        return builder;
    }

    List<StreamDataBlock> sortStreamRangePositions(Map<Long, List<StreamDataBlock>> streamDataBlocksMap) {
        //TODO: use merge sort
        Map<Long, List<StreamDataBlock>> sortedStreamObjectMap = new TreeMap<>();
        for (List<StreamDataBlock> streamDataBlocks : streamDataBlocksMap.values()) {
            streamDataBlocks.forEach(e -> sortedStreamObjectMap.computeIfAbsent(e.getStreamId(), k -> new ArrayList<>()).add(e));
        }
        return sortedStreamObjectMap.values().stream().flatMap(list -> {
            list.sort(StreamDataBlock.STREAM_OFFSET_COMPARATOR);
            return list.stream();
        }).collect(Collectors.toList());
    }

    private static abstract class AbstractCompactedObjectComparator implements Comparator<CompactedObjectBuilder> {
        protected final Map<Long, Integer> objectStatsMap;

        public AbstractCompactedObjectComparator(Map<Long, Integer> objectStatsMap) {
            this.objectStatsMap = objectStatsMap;
        }

        protected int compareCompactedObject(CompactedObjectBuilder o1, CompactedObjectBuilder o2) {
            return Integer.compare(CompactionUtils.getTotalObjectStats(o1, objectStatsMap),
                    CompactionUtils.getTotalObjectStats(o2, objectStatsMap));
        }
    }

    private static class CompactObjectComparator extends AbstractCompactedObjectComparator {
        public CompactObjectComparator(Map<Long, Integer> objectStatsMap) {
            super(objectStatsMap);
        }

        @Override
        public int compare(CompactedObjectBuilder o1, CompactedObjectBuilder o2) {
            int compare = Integer.compare(o1.totalStreamNum(), o2.totalStreamNum());
            if (compare == 0) {
                return compareCompactedObject(o1, o2);
            }
            return compare;
        }
    }

    private static class StreamObjectComparator extends AbstractCompactedObjectComparator {
        public StreamObjectComparator(Map<Long, Integer> objectStatsMap) {
            super(objectStatsMap);
        }

        @Override
        public int compare(CompactedObjectBuilder o1, CompactedObjectBuilder o2) {
            int compare = Integer.compare(o1.streamDataBlocks().size(), o2.streamDataBlocks().size());
            if (compare == 0) {
                return compareCompactedObject(o1, o2);
            }
            return compare;
        }
    }

}
