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

package com.automq.rocketmq.proxy.remoting;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;

public class RemotingUtil {
    // The response code that indicates that the request is not finished yet.
    public static final int REQUEST_NOT_FINISHED = -1;

    /**
     * Generates a code not supported response command.
     */
    public static RemotingCommand codeNotSupportedResponse(RemotingCommand request) {
        String error = " request type " + request.getCode() + " not supported";
        return RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, error);
    }

    /**
     * Generates a version not supported response command.
     */
    public static RemotingCommand versionNotSupportedResponse(RemotingCommand request) {
        String error = " request version " + request.getVersion() + " not supported";
        return RemotingCommand.createResponseCommand(ResponseCode.VERSION_NOT_SUPPORTED, error);
    }

    /**
     * Build a response command with the given response code, the opaque of the request, and the given custom header.
     * @param request The request command.
     * @param responseCode The response code.
     * @param classHeader The class of the custom header.
     * @return The response command.
     */
    public static RemotingCommand buildResponseCommand(RemotingCommand request, int responseCode,
        Class<? extends CommandCustomHeader> classHeader) {
        RemotingCommand response = RemotingCommand.createResponseCommand(responseCode, null, classHeader);
        response.setOpaque(request.getOpaque());
        return response;
    }

    /**
     * Build a response command with the given response code, and the opaque of the request.
     *
     * @param request The request command.
     * @param responseCode The response code.
     * @return The response command.
     */
    public static RemotingCommand buildResponseCommand(RemotingCommand request, int responseCode) {
        RemotingCommand response = RemotingCommand.createResponseCommand(responseCode, null, null);
        response.setOpaque(request.getOpaque());
        return response;
    }
}
