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

package com.automq.rocketmq.controller.metadata.database.serde;

import apache.rocketmq.controller.v1.SubStream;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SubStreamSerdeTest {

    @Test
    public void testSubStreamSerde() {
        Gson gson = new GsonBuilder()
            .registerTypeAdapter(SubStream.class, new SubStreamSerializer())
            .registerTypeAdapter(SubStream.class, new SubStreamDeserializer())
            .create();
        Map<Long, SubStream> map = new HashMap<>();
        map.put(1L, SubStream.newBuilder().setStreamId(1).setStartOffset(10).setEndOffset(20).build());
        map.put(2L, SubStream.newBuilder().setStreamId(2).setStartOffset(20).setEndOffset(40).build());
        String json = gson.toJson(map);

        Map<Long, SubStream> m = gson.fromJson(json, new TypeToken<>() {
        });
        assertEquals(2, m.size());
        LongStream.range(1, 3).mapToObj(m::get).forEach(subStream -> {
            assertEquals(subStream.getStartOffset(), subStream.getStreamId() * 10);
            assertEquals(subStream.getEndOffset(), subStream.getStreamId() * 20);
        });
    }
}