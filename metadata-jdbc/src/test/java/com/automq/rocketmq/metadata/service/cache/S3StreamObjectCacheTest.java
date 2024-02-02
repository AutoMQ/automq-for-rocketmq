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
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class S3StreamObjectCacheTest {

    @Test
    void listStreamObjects() {
        S3StreamObjectCache cache = new S3StreamObjectCache();

        List<S3StreamObject> list = new ArrayList<>();

        S3StreamObject streamObject = new S3StreamObject();
        streamObject.setStartOffset(0L);
        streamObject.setEndOffset(100L);
        list.add(streamObject);

        streamObject = new S3StreamObject();
        streamObject.setStartOffset(100L);
        streamObject.setEndOffset(200L);
        list.add(streamObject);

        cache.initStream(1, list);

        list = new ArrayList<>();

        streamObject = new S3StreamObject();
        streamObject.setStartOffset(200L);
        streamObject.setEndOffset(300L);
        list.add(streamObject);

        streamObject = new S3StreamObject();
        streamObject.setStartOffset(300L);
        streamObject.setEndOffset(400L);
        list.add(streamObject);

        cache.cache(1, list);

        List<S3StreamObject> result = cache.listStreamObjects(1, 0, 400, 100);
        assertEquals(4, result.size());

        result = cache.listStreamObjects(1, 50, 60, 100);
        assertEquals(1, result.size());
        assertEquals(0L, result.get(0).getStartOffset());

        result = cache.listStreamObjects(1, 250, 400, 1);
        assertEquals(1, result.size());
        assertEquals(200L, result.get(0).getStartOffset());

        result = cache.listStreamObjects(1, 150, -1, 2);
        assertEquals(2, result.size());
        assertEquals(100L, result.get(0).getStartOffset());
        assertEquals(200L, result.get(1).getStartOffset());
    }
}