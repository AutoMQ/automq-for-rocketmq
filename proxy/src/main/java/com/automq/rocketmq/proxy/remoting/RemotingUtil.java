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

package com.automq.rocketmq.proxy.remoting;

import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.processor.channel.ChannelProtocolType;
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
        String error = "request type " + request.getCode() + " not supported";
        return RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, error);
    }

    /**
     * Generates a version not supported response command.
     */
    public static RemotingCommand clientNotSupportedResponse(RemotingCommand request) {
        String error = request.getLanguage().name() + " client version " + MQVersion.getVersionDesc(request.getVersion()) + " not supported, please use JAVA client v4.9.5 or later.";
        return RemotingCommand.createResponseCommand(ResponseCode.VERSION_NOT_SUPPORTED, error);
    }

    /**
     * Build a response command with the given response code, the opaque of the request, and the given custom header.
     *
     * @param request      The request command.
     * @param responseCode The response code.
     * @param classHeader  The class of the custom header.
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
     * @param request      The request command.
     * @param responseCode The response code.
     * @return The response command.
     */
    public static RemotingCommand buildResponseCommand(RemotingCommand request, int responseCode) {
        RemotingCommand response = RemotingCommand.createResponseCommand(responseCode, null, null);
        response.setOpaque(request.getOpaque());
        return response;
    }

    public static boolean isRemotingProtocol(ProxyContext ctx) {
        return ChannelProtocolType.REMOTING.getName().equals(ctx.getProtocolType());
    }
}
