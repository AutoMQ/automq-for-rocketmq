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

package com.automq.rocketmq.metadata.service.cache;

import com.automq.rocketmq.metadata.dao.S3StreamObject;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class S3StreamObjectCache {

    /**
     * StreamID --> List
     */
    private final ConcurrentMap<Long, List<S3StreamObject>> s3StreamObjects;

    /**
     * A stream is non-exclusive if it is present in WAL_OBJECTS on different nodes.
     */
    private final ConcurrentMap<Long, Long> nonExclusiveStreams;

    private final ConcurrentMap<Long, ReadWriteLock> streamLocks;

    public S3StreamObjectCache() {
        s3StreamObjects = new ConcurrentHashMap<>();
        nonExclusiveStreams = new ConcurrentHashMap<>();
        streamLocks = new ConcurrentHashMap<>();
    }

    public boolean streamExclusive(long streamId) {
        return !nonExclusiveStreams.containsKey(streamId);
    }

    public void makeStreamExclusive(long streamId) {
        nonExclusiveStreams.remove(streamId);
    }

    public void initStream(long streamId, List<S3StreamObject> list) {
        list.sort(Comparator.comparingLong(S3StreamObject::getStartOffset));
        s3StreamObjects.putIfAbsent(streamId, list);
        streamLocks.putIfAbsent(streamId, new ReentrantReadWriteLock());
    }

    public void onStreamClose(long streamId) {
        s3StreamObjects.remove(streamId);
        streamLocks.remove(streamId);
    }

    public List<Long> onTrim(long streamId, long offset) {
        List<S3StreamObject> list = s3StreamObjects.getOrDefault(streamId, new ArrayList<>());
        ReadWriteLock lock = streamLocks.get(streamId);
        if (null == lock) {
            return new ArrayList<>();
        }

        lock.writeLock().lock();
        try {
            List<S3StreamObject> trimmed = list.stream().filter(streamObject -> streamObject.getEndOffset() < offset)
                .toList();
            list.removeAll(trimmed);
            return trimmed.stream().map(S3StreamObject::getObjectId).toList();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void cache(long streamId, List<S3StreamObject> items) {
        if (null == items || items.isEmpty()) {
            return;
        }

        List<S3StreamObject> list = s3StreamObjects.getOrDefault(streamId, List.of());
        ReadWriteLock lock = streamLocks.get(streamId);
        if (null == lock) {
            return;
        }

        lock.writeLock().lock();
        try {
            list.addAll(items);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void onCompact(long streamId, List<Long> objectIds) {
        List<S3StreamObject> list = s3StreamObjects.getOrDefault(streamId, List.of());
        ReadWriteLock lock = streamLocks.get(streamId);
        if (null == lock) {
            return;
        }

        lock.writeLock().lock();
        try {
            list.removeIf(item -> objectIds.contains(item.getObjectId()));
        } finally {
            lock.writeLock().unlock();
        }
    }

    public List<S3StreamObject> listStreamObjects(long streamId, long startOffset, long endOffset, int limit) {
        List<S3StreamObject> list = s3StreamObjects.getOrDefault(streamId, new ArrayList<>());
        ReadWriteLock lock = streamLocks.get(streamId);
        if (null == lock) {
            return list;
        }

        lock.readLock().lock();
        try {
            List<S3StreamObject> reversed = Lists.reverse(list);
            return reversed.stream()
                .filter(s3StreamObject -> s3StreamObject.getEndOffset() >= startOffset
                    && s3StreamObject.getStartOffset() <= endOffset)
                .limit(limit)
                .toList();
        } finally {
            lock.readLock().unlock();
        }

    }

}
