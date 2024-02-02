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
import apache.rocketmq.v2.Status;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;

public class ExceptionHandler {
    private static final Map<Code, Integer> PROXY_EXCEPTION_RESPONSE_CODE_MAP = new HashMap<>() {
        {
            put(Code.FORBIDDEN, ResponseCode.NO_PERMISSION);
            put(Code.TOPIC_NOT_FOUND, ResponseCode.TOPIC_NOT_EXIST);
            put(Code.CONSUMER_GROUP_NOT_FOUND, ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
            put(Code.MESSAGE_NOT_FOUND, ResponseCode.NO_MESSAGE);
            put(Code.MESSAGE_PROPERTY_CONFLICT_WITH_TYPE, ResponseCode.MESSAGE_ILLEGAL);
            put(Code.INTERNAL_ERROR, ResponseCode.SYSTEM_ERROR);
            put(Code.INTERNAL_SERVER_ERROR, ResponseCode.SYSTEM_ERROR);
        }
    };

    public static Optional<RemotingCommand> convertToRemotingResponse(Throwable throwable) {
        if (throwable instanceof ProxyException proxyException) {
            Integer responseCode = PROXY_EXCEPTION_RESPONSE_CODE_MAP.getOrDefault(proxyException.getErrorCode(), ResponseCode.SYSTEM_ERROR);
            RemotingCommand response = RemotingCommand.buildErrorResponse(responseCode, proxyException.getMessage());
            return Optional.of(response);
        }
        return Optional.empty();
    }

    public static Optional<Status> convertToGrpcStatus(Throwable throwable) {
        if (throwable instanceof ProxyException proxyException) {
            return Optional.of(
                Status.newBuilder()
                    .setCode(proxyException.getErrorCode())
                    .setMessage(proxyException.getMessage())
                    .build()
            );
        }
        return Optional.empty();
    }
}
