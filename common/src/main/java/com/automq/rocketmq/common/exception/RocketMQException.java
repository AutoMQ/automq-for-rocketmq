/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.rocketmq.common.exception;

public class RocketMQException extends Exception {

    private final int errorCode;

    public RocketMQException(int errorCode) {
        this.errorCode = errorCode;
    }

    public RocketMQException(int errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public RocketMQException(int errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public RocketMQException(int errorCode, Throwable cause) {
        super(cause);
        this.errorCode = errorCode;
    }

    public RocketMQException(int errorCode, String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
        this.errorCode = errorCode;
    }

    public int getErrorCode() {
        return errorCode;
    }
}
