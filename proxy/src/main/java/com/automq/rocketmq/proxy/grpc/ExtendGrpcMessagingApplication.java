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
