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

public class ControllerException extends RocketMQException {
    public ControllerException(int errorCode, String message) {
        super(errorCode, message);
    }

    public ControllerException(int errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }

    public ControllerException(int errorCode, Throwable cause) {
        super(errorCode, cause);
    }

    public ControllerException(int errorCode, String message, Throwable cause, boolean enableSuppression,
        boolean writableStackTrace) {
        super(errorCode, message, cause, enableSuppression, writableStackTrace);
    }
}
