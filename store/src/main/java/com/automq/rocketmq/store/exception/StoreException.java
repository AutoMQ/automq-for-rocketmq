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

public class StoreException extends Exception {
    private final StoreErrorCode code;

    public StoreException(StoreErrorCode code, String message) {
        super(message);
        this.code = code;
    }

    public StoreException(StoreErrorCode code, String message, Throwable source) {
        super(message, source);
        this.code = code;
    }

    public StoreErrorCode code() {
        return code;
    }

    public String message() {
        return getMessage();
    }

    public Throwable source() {
        return getCause();
    }
}
