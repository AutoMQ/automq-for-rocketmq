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
