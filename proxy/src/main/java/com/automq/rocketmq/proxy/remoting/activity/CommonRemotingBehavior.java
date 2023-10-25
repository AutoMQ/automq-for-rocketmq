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
        ProxyMetricsManager.recordRpcLatency(context.getProtocolType(), context.getAction(),
            RemotingHelper.getResponseCodeDesc(response.getCode()), ((ProxyContextExt) context).getElapsedTimeNanos());
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
