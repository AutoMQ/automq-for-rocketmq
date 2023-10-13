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

package com.automq.rocketmq.store.api;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Provide S3 object management, such as delete.
 */
public interface S3ObjectOperator {
    /**
     * Delete a list of S3 objects by object id.
     * <p>
     * Regard non-exist object as success delete.
     *
     * @param objectIds the objects to delete.
     * @return the future of delete result, contains the deleted object id.
     */
    CompletableFuture<List<Long>> delete(List<Long> objectIds);
}
