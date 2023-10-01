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

package com.automq.rocketmq.store.model.operation;

import com.automq.rocketmq.store.model.metadata.ConsumerGroupMetadata;
import java.util.List;

public class OperationSnapshot {

    private final long snapshotEndOffset;
    private final List<PopOperation> popOperations;
    private final List<ConsumerGroupMetadata> consumerGroupMetadataList;

    public OperationSnapshot(long snapshotEndOffset, List<PopOperation> popOperations, List<ConsumerGroupMetadata> consumerGroupMetadataList) {
        this.snapshotEndOffset = snapshotEndOffset;
        this.popOperations = popOperations;
        this.consumerGroupMetadataList = consumerGroupMetadataList;
    }

    public long getSnapshotEndOffset() {
        return snapshotEndOffset;
    }

    public List<PopOperation> getPopOperations() {
        return popOperations;
    }

    public List<ConsumerGroupMetadata> getConsumerGroupMetadataList() {
        return consumerGroupMetadataList;
    }

}
