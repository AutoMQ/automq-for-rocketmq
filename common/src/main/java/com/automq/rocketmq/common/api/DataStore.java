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

package com.automq.rocketmq.common.api;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface DataStore {

    CompletableFuture<Void> closeQueue(long topicId, int queueId);

    /**
     * Trim stream such that records with offsets prior to <code>offset</code> will be inaccessible.
     *
     * @param streamId ID of the stream to trim
     * @param offset   Minimum offset of the stream after trim operation
     * @return True if successful; failure future otherwise.
     */
    CompletableFuture<Boolean> trimStream(long streamId, long offset);

    /**
     * Delete a list of S3 objects by object id.
     * <p>
     * Regard non-exist object as success delete.
     *
     * @param objectIds the objects to delete.
     * @return the future of delete result, contains the deleted object id.
     */
    CompletableFuture<List<Long>> batchDeleteS3Objects(List<Long> objectIds);

    default void blockingShutdown() {

    }

}
