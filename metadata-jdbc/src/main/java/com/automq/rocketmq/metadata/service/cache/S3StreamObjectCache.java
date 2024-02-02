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

package com.automq.rocketmq.metadata.service.cache;

import com.automq.rocketmq.metadata.dao.S3StreamObject;
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
            return list.stream()
                .filter(s3StreamObject -> s3StreamObject.getEndOffset() >= startOffset && (s3StreamObject.getStartOffset() <= endOffset || endOffset == -1))
                .limit(limit)
                .toList();
        } finally {
            lock.readLock().unlock();
        }
    }

    public long streamStartTime(long streamId) {
        List<S3StreamObject> objs = s3StreamObjects.get(streamId);
        long startTime = System.currentTimeMillis();
        if (null == objs) {
            return startTime;
        }
        for (S3StreamObject obj : objs) {
            long ts = obj.getBaseDataTimestamp().getTime();
            if (ts < startTime) {
                startTime = ts;
            }
        }
        return startTime;
    }

}
