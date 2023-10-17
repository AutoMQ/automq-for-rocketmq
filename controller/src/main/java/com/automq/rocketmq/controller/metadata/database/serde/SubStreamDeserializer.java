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
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import java.lang.reflect.Type;

public class SubStreamDeserializer implements JsonDeserializer<SubStream> {
    @Override
    public SubStream deserialize(JsonElement element, Type type, JsonDeserializationContext context)
        throws JsonParseException {
        SubStream.Builder builder = SubStream.newBuilder();
        if (element.isJsonObject()) {
            JsonObject root = (JsonObject) element;
            if (root.has("streamId")) {
                builder.setStreamId(root.get("streamId").getAsLong());
            }

            if (root.has("startOffset")) {
                builder.setStartOffset(root.get("startOffset").getAsLong());
            }

            if (root.has("endOffset")) {
                builder.setEndOffset(root.get("endOffset").getAsLong());
            }
        }
        return builder.build();
    }
}
