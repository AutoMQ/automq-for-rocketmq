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

import apache.rocketmq.v2.AckMessageResponse;
import apache.rocketmq.v2.ChangeInvisibleDurationResponse;
import apache.rocketmq.v2.EndTransactionResponse;
import apache.rocketmq.v2.HeartbeatResponse;
import apache.rocketmq.v2.QueryRouteResponse;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.SendMessageResponse;
import apache.rocketmq.v2.Status;
import com.automq.rocketmq.proxy.exception.ExceptionHandler;
import com.automq.rocketmq.proxy.metrics.ProxyMetricsManager;
import com.automq.rocketmq.proxy.model.ProxyContextExt;
import io.grpc.stub.StreamObserver;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.function.Function;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.GrpcMessagingApplication;
import org.apache.rocketmq.proxy.grpc.v2.GrpcMessingActivity;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseWriter;

public class ExtendGrpcMessagingApplication extends GrpcMessagingApplication {
    public ExtendGrpcMessagingApplication(GrpcMessingActivity grpcMessingActivity) {
        super(grpcMessingActivity);
    }

    @Override
    protected ProxyContext createContext() {
        return ProxyContextExt.create(super.createContext());
    }

    private <T> String getResponseStatus(T response) {
        if (response == null) {
            return "unknown";
        }

        if (response instanceof SendMessageResponse detailResponse) {
            return detailResponse.getStatus().getCode().name().toLowerCase();
        } else if (response instanceof ReceiveMessageResponse detailResponse) {
            return detailResponse.getStatus().getCode().name().toLowerCase();
        } else if (response instanceof AckMessageResponse detailResponse) {
            return detailResponse.getStatus().getCode().name().toLowerCase();
        } else if (response instanceof ChangeInvisibleDurationResponse detailResponse) {
            return detailResponse.getStatus().getCode().name().toLowerCase();
        } else if (response instanceof EndTransactionResponse detailResponse) {
            return detailResponse.getStatus().getCode().name().toLowerCase();
        } else if (response instanceof QueryRouteResponse detailResponse) {
            return detailResponse.getStatus().getCode().name().toLowerCase();
        } else if (response instanceof HeartbeatResponse detailResponse) {
            return detailResponse.getStatus().getCode().name().toLowerCase();
        }

        try {
            Method getStatus = response.getClass().getDeclaredMethod("getStatus");
            Status status = (Status) getStatus.invoke(response);
            return status.getCode().name().toLowerCase();
        } catch (Exception e) {
            return "unknown";
        }
    }

    @Override
    protected <V, T> void writeResponse(ProxyContext context, V request, T response, StreamObserver<T> responseObserver,
        Throwable t, Function<Status, T> errorResponseCreator) {
        ProxyContextExt contextExt = (ProxyContextExt) context;
        if (t != null) {
            Optional<Status> status = ExceptionHandler.convertToGrpcStatus(t);
            if (status.isPresent()) {
                ProxyMetricsManager.recordRpcLatency(context.getProtocolType(), context.getAction(),
                    status.get().getCode().name().toLowerCase(), contextExt.getElapsedTimeNanos(), contextExt.suspended(), contextExt.relayed());
                ResponseWriter.getInstance().write(
                    responseObserver,
                    errorResponseCreator.apply(status.get())
                );
                return;
            }
        }

        ProxyMetricsManager.recordRpcLatency(context.getProtocolType(), context.getAction(),
            getResponseStatus(response), contextExt.getElapsedTimeNanos(), contextExt.suspended(), contextExt.relayed());

        super.writeResponse(context, request, response, responseObserver, t, errorResponseCreator);
    }
}
