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
