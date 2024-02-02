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

package com.automq.rocketmq.common.exception;

public class RocketMQRuntimeException extends RuntimeException {
    protected final int errorCode;

    public RocketMQRuntimeException(int errorCode) {
        this.errorCode = errorCode;
    }

    public RocketMQRuntimeException(int errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public RocketMQRuntimeException(int errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public RocketMQRuntimeException(int errorCode, Throwable cause) {
        super(cause);
        this.errorCode = errorCode;
    }

    public RocketMQRuntimeException(int errorCode, String message, Throwable cause, boolean enableSuppression,
        boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
        this.errorCode = errorCode;
    }
}
