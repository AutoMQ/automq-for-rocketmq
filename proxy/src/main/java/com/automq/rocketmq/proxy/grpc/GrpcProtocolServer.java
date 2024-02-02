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

import com.automq.rocketmq.common.config.ProxyConfig;
import com.automq.rocketmq.common.util.Lifecycle;
import com.automq.rocketmq.controller.server.ControllerServiceImpl;
import com.automq.rocketmq.proxy.grpc.activity.ExtendGrpcMessagingActivity;
import io.grpc.protobuf.services.ChannelzService;
import io.grpc.protobuf.services.ProtoReflectionService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.GrpcServer;
import org.apache.rocketmq.proxy.grpc.GrpcServerBuilder;
import org.apache.rocketmq.proxy.grpc.v2.GrpcMessagingApplication;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The protocol server provides the ability to support multiple protocols.
 * <p>
 * 1. The newest gRPC protocol introduced in Apache RocketMQ 5.0, see <a href="https://github.com/apache/rocketmq-apis/">RocketMQ APIs</a>.
 * 2. The classic remoting protocol born with RocketMQ.
 */
public class GrpcProtocolServer implements Lifecycle {
    private static final Logger LOGGER = LoggerFactory.getLogger(GrpcProtocolServer.class);
    private final GrpcServer grpcServer;
    private final ThreadPoolExecutor grpcExecutor;
    private final GrpcMessagingApplication grpcMessagingApplication;

    public GrpcProtocolServer(ProxyConfig config, MessagingProcessor messagingProcessor,
        ControllerServiceImpl controllerService, ProxyServiceImpl proxyService) {
        grpcExecutor = createGrpcExecutor(config.grpcThreadPoolNums(), config.grpcThreadPoolQueueCapacity());
        grpcMessagingApplication = createServiceProcessor(messagingProcessor);
        grpcServer = GrpcServerBuilder.newBuilder(grpcExecutor, ConfigurationManager.getProxyConfig().getGrpcServerPort())
            .addService(grpcMessagingApplication)
            .addService(ChannelzService.newInstance(100))
            .addService(ProtoReflectionService.newInstance())
            .addService(controllerService)
            .addService(proxyService)
            .configInterceptor()
            .build();
    }

    @Override
    public void start() throws Exception {
        this.grpcMessagingApplication.start();
        this.grpcServer.start();
    }

    @Override
    public void shutdown() throws Exception {
        this.grpcMessagingApplication.shutdown();
        this.grpcServer.shutdown();
        this.grpcExecutor.shutdown();
    }

    private GrpcMessagingApplication createServiceProcessor(MessagingProcessor messagingProcessor) {
        return new ExtendGrpcMessagingApplication(new ExtendGrpcMessagingActivity(messagingProcessor));
    }

    private ThreadPoolExecutor createGrpcExecutor(int threadNums, int queueCapacity) {
        return ThreadPoolMonitor.createAndMonitor(
            threadNums,
            threadNums,
            1, TimeUnit.MINUTES,
            "GrpcRequestExecutorThread",
            queueCapacity
        );
    }
}
