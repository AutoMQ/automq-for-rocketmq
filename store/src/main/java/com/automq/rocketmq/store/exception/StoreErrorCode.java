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

package com.automq.rocketmq.store.exception;

public enum StoreErrorCode {
    FILE_SYSTEM_PERMISSION,
    KV_SERVICE_IS_NOT_RUNNING,
    ILLEGAL_ARGUMENT,
    KV_ENGINE_ERROR,
    QUEUE_NOT_OPENED,
    QUEUE_FENCED,
    QUEUE_NOT_FOUND,
    QUEUE_OPENING,
    INNER_ERROR,
    DATA_CORRUPTED,
}
