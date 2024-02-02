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

package com.automq.rocketmq.proxy.remoting.activity;

import com.automq.rocketmq.proxy.metrics.ProxyMetricsManager;
import com.automq.rocketmq.proxy.model.ProxyContextExt;
import com.automq.rocketmq.proxy.remoting.RemotingUtil;
import java.util.Optional;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;

/**
 * Define some common behaviors for remoting processors.
 */
public interface CommonRemotingBehavior {
    String BROKER_NAME_FIELD = "bname";
    String BROKER_NAME_FIELD_FOR_SEND_MESSAGE_V2 = "n";

    default void recordRpcLatency(ProxyContext context, RemotingCommand response) {
        ProxyContextExt contextExt = (ProxyContextExt) context;
        ProxyMetricsManager.recordRpcLatency(context.getProtocolType(), context.getAction(),
            RemotingHelper.getResponseCodeDesc(response.getCode()), contextExt.getElapsedTimeNanos(), contextExt.suspended(), contextExt.relayed());
    }

    default ProxyContext createExtendContext(ProxyContext context) {
        return ProxyContextExt.create(context);
    }

    /**
     * Retrieve the destination broker name from the request.
     *
     * @param request The remoting request.
     * @return The destination broker name
     */
    default String dstBrokerName(RemotingCommand request) {
        if (request.getCode() == RequestCode.SEND_MESSAGE_V2) {
            return request.getExtFields().get(BROKER_NAME_FIELD_FOR_SEND_MESSAGE_V2);
        } else {
            return request.getExtFields().get(BROKER_NAME_FIELD);
        }
    }

    default Optional<RemotingCommand> checkClientVersion(RemotingCommand request) {
        if (request.getLanguage() != LanguageCode.JAVA) {
            return Optional.of(RemotingUtil.clientNotSupportedResponse(request));
        }

        if (request.getVersion() < MQVersion.Version.V4_9_5.ordinal()) {
            return Optional.of(RemotingUtil.clientNotSupportedResponse(request));
        }
        return Optional.empty();
    }

    /**
     * Check the version of the request.
     * <p>
     * Since the {@link RemotingCommand#getVersion()} is not reliable to check whether the request have the bname field,
     * so we check the real request fields here.
     *
     * @param request The remoting request.
     * @return The error response or null if the version is supported.
     */
    default Optional<RemotingCommand> checkRequiredField(RemotingCommand request) {
        if (requestNeedBrokerName(request.getCode())) {
            String brokerName;
            if (request.getCode() == RequestCode.SEND_MESSAGE_V2) {
                brokerName = request.getExtFields().get(BROKER_NAME_FIELD_FOR_SEND_MESSAGE_V2);
            } else {
                brokerName = request.getExtFields().get(BROKER_NAME_FIELD);
            }
            if (brokerName == null) {
                return Optional.of(RemotingUtil.clientNotSupportedResponse(request));
            }
        }
        return Optional.empty();
    }

    default boolean requestNeedBrokerName(int requestCode) {
        return requestCode == RequestCode.SEND_MESSAGE_V2
            || requestCode == RequestCode.PULL_MESSAGE
            || requestCode == RequestCode.CONSUMER_SEND_MSG_BACK;
    }
}
