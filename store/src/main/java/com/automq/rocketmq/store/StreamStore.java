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

package com.automq.rocketmq.store;

import com.automq.rocketmq.stream.api.AppendResult;
import com.automq.rocketmq.stream.api.FetchResult;
import com.automq.rocketmq.stream.api.RecordBatch;
import java.util.concurrent.CompletableFuture;

/**
 * A high level abstraction of stream store, hidden the details of S3Stream module.
 */
public interface StreamStore {
    /**
     * Fetch records from stream store.
     *
     * @param streamId the target stream id.
     * @param startOffset the start offset of the fetch.
     * @param maxCount the max return count of the fetch.
     * @return the future of fetch result.
     */
    CompletableFuture<FetchResult> fetch(long streamId, long startOffset, int maxCount);

    /**
     * Append record batch to stream store.
     * @param streamId the target stream id.
     * @param recordBatch the record batch to append.
     * @return the future of append result.
     */
    CompletableFuture<AppendResult> append(long streamId, RecordBatch recordBatch);
}
