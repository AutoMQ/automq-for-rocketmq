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

import apache.rocketmq.controller.v1.S3StreamSetObject;
import apache.rocketmq.controller.v1.SubStream;
import com.automq.rocketmq.metadata.mapper.S3StreamSetObjectMapper;
import com.automq.rocketmq.metadata.service.Helper;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;

public class S3StreamSetObjectCache {

    private final ConcurrentMap<Long, S3StreamSetObject> cache;
    private final SqlSessionFactory sessionFactory;

    public S3StreamSetObjectCache(SqlSessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
        cache = new ConcurrentHashMap<>();
    }

    public void load(int nodeId) {
        try (SqlSession session = sessionFactory.openSession()) {
            S3StreamSetObjectMapper mapper = session.getMapper(S3StreamSetObjectMapper.class);
            List<com.automq.rocketmq.metadata.dao.S3StreamSetObject> list = mapper.list(nodeId, null);
            list.forEach(obj -> {
                try {
                    cache.put(obj.getObjectId(), Helper.buildS3StreamSetObject(obj));
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    public void onCompact(Collection<Long> objectIds) {
        if (null == objectIds || objectIds.isEmpty()) {
            return;
        }
        objectIds.forEach(cache::remove);
    }

    public void onCommit(S3StreamSetObject object) {
        cache.put(object.getObjectId(), object);
    }

    public long streamDataSize(long streamId) {
        long total = 0;
        for (Map.Entry<Long, S3StreamSetObject> entry : cache.entrySet()) {
            for (Map.Entry<Long, SubStream> e : entry.getValue().getSubStreams().getSubStreamsMap().entrySet()) {
                if (e.getValue().getStreamId() == streamId) {
                    total += e.getValue().getDataSize();
                }
            }
        }
        return total;
    }

    public long streamStartTime(long streamId) {
        long startTime = System.currentTimeMillis();
        for (Map.Entry<Long, S3StreamSetObject> entry : cache.entrySet()) {
            for (Map.Entry<Long, SubStream> e : entry.getValue().getSubStreams().getSubStreamsMap().entrySet()) {
                if (e.getValue().getStreamId() == streamId) {
                    if (entry.getValue().getBaseDataTimestamp() < startTime) {
                        startTime = entry.getValue().getBaseDataTimestamp();
                    }
                }
            }
        }
        return startTime;
    }
}
