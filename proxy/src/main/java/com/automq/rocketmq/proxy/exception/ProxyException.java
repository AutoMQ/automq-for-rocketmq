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

package com.automq.rocketmq.proxy.exception;

import apache.rocketmq.v2.Code;
import com.automq.rocketmq.common.exception.RocketMQRuntimeException;

public class ProxyException extends RocketMQRuntimeException {
    public ProxyException(Code errorCode) {
        super(errorCode.getNumber());
    }

    public ProxyException(Code errorCode, String message) {
        super(errorCode.getNumber(), message);
    }

    public ProxyException(Code errorCode, String message, Throwable cause) {
        super(errorCode.getNumber(), message, cause);
    }

    public ProxyException(Code errorCode, Throwable cause) {
        super(errorCode.getNumber(), cause);
    }

    public ProxyException(Code errorCode, String message, Throwable cause, boolean enableSuppression,
        boolean writableStackTrace) {
        super(errorCode.getNumber(), message, cause, enableSuppression, writableStackTrace);
    }

    public Code getErrorCode() {
        return Code.forNumber(errorCode);
    }
}
