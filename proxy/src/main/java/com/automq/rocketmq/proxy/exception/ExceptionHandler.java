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
