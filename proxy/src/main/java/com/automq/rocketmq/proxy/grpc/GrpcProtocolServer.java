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
