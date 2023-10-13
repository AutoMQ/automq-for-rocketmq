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

package com.automq.rocketmq.proxy.grpc;

import apache.rocketmq.v2.Status;
import com.automq.rocketmq.proxy.metrics.ProxyMetricsManager;
import com.automq.rocketmq.proxy.model.ProxyContextExt;
import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.stub.StreamObserver;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.interceptor.InterceptorConstants;
import org.apache.rocketmq.proxy.grpc.v2.GrpcMessagingApplication;
import org.apache.rocketmq.proxy.grpc.v2.GrpcMessingActivity;
import org.apache.rocketmq.proxy.processor.channel.ChannelProtocolType;

public class ExtendGrpcMessagingApplication extends GrpcMessagingApplication {
    public ExtendGrpcMessagingApplication(GrpcMessingActivity grpcMessingActivity) {
        super(grpcMessingActivity);
    }

    @Override
    protected ProxyContext createContext() {
        Context ctx = Context.current();
        Metadata headers = InterceptorConstants.METADATA.get(ctx);
        ProxyContext context = new ProxyContextExt()
            .setLocalAddress(getDefaultStringMetadataInfo(headers, InterceptorConstants.LOCAL_ADDRESS))
            .setRemoteAddress(getDefaultStringMetadataInfo(headers, InterceptorConstants.REMOTE_ADDRESS))
            .setClientID(getDefaultStringMetadataInfo(headers, InterceptorConstants.CLIENT_ID))
            .setProtocolType(ChannelProtocolType.GRPC_V2.getName())
            .setLanguage(getDefaultStringMetadataInfo(headers, InterceptorConstants.LANGUAGE))
            .setClientVersion(getDefaultStringMetadataInfo(headers, InterceptorConstants.CLIENT_VERSION))
            .setAction(getDefaultStringMetadataInfo(headers, InterceptorConstants.SIMPLE_RPC_NAME));
        if (ctx.getDeadline() != null) {
            context.setRemainingMs(ctx.getDeadline().timeRemaining(TimeUnit.MILLISECONDS));
        }
        return context;
    }

    private <T> String getResponseStatus(T response) {
        try {
            Method getStatus = response.getClass().getDeclaredMethod("getStatus");
            Status status = (Status) getStatus.invoke(response);
            return status.getMessage();
        } catch (Exception e) {
            return "unknown";
        }
    }

    @Override
    protected <V, T> void writeResponse(ProxyContext context, V request, T response, StreamObserver<T> responseObserver,
        Throwable t, Function<Status, T> errorResponseCreator) {
        ProxyMetricsManager.recordRpcLatency(context.getProtocolType(), context.getAction(),
            getResponseStatus(response), ((ProxyContextExt) context).getElapsedTimeNanos());
        super.writeResponse(context, request, response, responseObserver, t, errorResponseCreator);
    }
}
