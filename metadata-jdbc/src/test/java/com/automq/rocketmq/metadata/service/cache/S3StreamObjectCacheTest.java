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

        list.clear();

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

        result = cache.listStreamObjects(1, 200, 400, 1);
        assertEquals(1, result.size());
        assertEquals(200L, result.get(0).getStartOffset());

        result = cache.listStreamObjects(1, 200, -1, 2);
        assertEquals(2, result.size());
        assertEquals(200L, result.get(0).getStartOffset());
        assertEquals(300L, result.get(1).getStartOffset());
    }
}