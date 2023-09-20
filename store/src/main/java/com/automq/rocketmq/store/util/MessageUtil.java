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

package com.automq.rocketmq.store.util;

import com.automq.rocketmq.common.model.MessageExt;
import com.automq.rocketmq.common.model.generated.KeyValue;
import com.automq.rocketmq.common.model.generated.Message;
import com.automq.rocketmq.stream.api.RecordBatchWithContext;
import com.google.flatbuffers.FlatBufferBuilder;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public class MessageUtil {
    public static MessageExt transferToMessageExt(RecordBatchWithContext recordBatch) {
        Message message = Message.getRootAsMessage(recordBatch.rawPayload());
        return MessageExt.Builder.builder()
            .message(message)
            .offset(recordBatch.baseOffset())
            .systemProperties(recordBatch.properties())
            .build();
    }

    public static Message transferToMessage(long topicId, int queueId, String tag,
        Map<String, String> userProperties, byte[] body) {
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int[] userPropertiesOffsets = userProperties.entrySet()
            .stream()
            .map(entry -> KeyValue.createKeyValue(builder,
                builder.createString(entry.getKey()), builder.createString(entry.getValue())))
            .mapToInt(Integer::valueOf)
            .toArray();
        int root = Message.createMessage(builder, topicId, queueId,
            builder.createString(StringUtils.isNotBlank(tag) ? tag : ""),
            builder.createVectorOfTables(userPropertiesOffsets),
            builder.createByteVector(body));
        builder.finish(root);
        return Message.getRootAsMessage(builder.dataBuffer());
    }
}
