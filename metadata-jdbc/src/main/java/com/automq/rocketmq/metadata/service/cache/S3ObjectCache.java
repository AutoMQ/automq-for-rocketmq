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

import apache.rocketmq.controller.v1.S3ObjectState;
import com.automq.rocketmq.metadata.dao.S3Object;
import com.automq.rocketmq.metadata.dao.S3ObjectCriteria;
import com.automq.rocketmq.metadata.mapper.S3ObjectMapper;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;

public class S3ObjectCache {
    private final SqlSessionFactory sessionFactory;
    private final ConcurrentMap<Long/*StreamId*/, ConcurrentMap<Long/*ObjectId*/, S3Object>> cache;

    public S3ObjectCache(SqlSessionFactory sessionFactory) {
        cache = new ConcurrentHashMap<>();
        this.sessionFactory = sessionFactory;
    }

    public void onStreamOpen(long streamId) {
        try (SqlSession session = sessionFactory.openSession()) {
            S3ObjectMapper mapper = session.getMapper(S3ObjectMapper.class);
            S3ObjectCriteria criteria = S3ObjectCriteria.newBuilder()
                .withState(S3ObjectState.BOS_COMMITTED)
                .withStreamId(streamId)
                .build();
            List<S3Object> list = mapper.list(criteria);
            cache.computeIfAbsent(streamId, k -> {
                ConcurrentMap<Long, S3Object> map = new ConcurrentHashMap<>();
                list.forEach(obj -> map.put(obj.getId(), obj));
                return map;
            });
        }
    }

    public void onObjectDelete(long streamId, Collection<Long> objectIds) {
        if (null == objectIds || objectIds.isEmpty()) {
            return;
        }

        cache.computeIfPresent(streamId, (k, m) -> {
            objectIds.forEach(m::remove);
            return m;
        });
    }

    public void onObjectAdd(Collection<S3Object> objects) {
        objects.forEach(object -> {
            if (!cache.containsKey(object.getStreamId())) {
                return;
            }

            cache.get(object.getStreamId()).put(object.getId(), object);
        });
    }

    public void onStreamClose(long streamId) {
        cache.remove(streamId);
    }

    public long streamDataSize(long streamId) {
        Map<Long, S3Object> objs = cache.get(streamId);
        if (null == objs) {
            return 0;
        }
        return objs.values().stream().mapToLong(S3Object::getObjectSize).sum();
    }

    public long streamStartTime(long streamId) {
        Map<Long, S3Object> objs = cache.get(streamId);
        long startTime = System.currentTimeMillis();
        if (null == objs) {
            return startTime;
        }
        for (Map.Entry<Long, S3Object> entry : objs.entrySet()) {
            long ts = entry.getValue().getCommittedTimestamp().getTime();
            if (ts < startTime) {
                startTime = ts;
            }
        }
        return startTime;
    }
}
