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

import com.automq.rocketmq.common.model.FlatMessageExt;
import com.automq.rocketmq.common.model.generated.FlatMessage;
import com.automq.stream.api.RecordBatchWithContext;

/**
 * An utility class to convert S3Stream Record to FlatMessage, and vice versa.
 */
public class FlatMessageUtil {
    public static FlatMessageExt transferToMessageExt(RecordBatchWithContext recordBatch) {
        FlatMessage message = FlatMessage.getRootAsFlatMessage(recordBatch.rawPayload());
        return FlatMessageExt.Builder.builder()
            .message(message)
            .offset(recordBatch.baseOffset())
            .build();
    }
}
