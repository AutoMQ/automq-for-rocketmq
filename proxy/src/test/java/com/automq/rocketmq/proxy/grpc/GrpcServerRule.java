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

import com.google.common.base.Preconditions;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.util.MutableHandlerRegistry;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public final class GrpcServerRule implements BeforeEachCallback, AfterEachCallback {
    private ManagedChannel channel;
    private Server server;
    private String serverName;
    private MutableHandlerRegistry serviceRegistry;
    private boolean useDirectExecutor;

    public GrpcServerRule() {
    }

    public GrpcServerRule directExecutor() {
        Preconditions.checkState(this.serverName == null, "directExecutor() can only be called at the rule instantiation");
        this.useDirectExecutor = true;
        return this;
    }

    public ManagedChannel getChannel() {
        return this.channel;
    }

    public Server getServer() {
        return this.server;
    }

    public String getServerName() {
        return this.serverName;
    }

    public MutableHandlerRegistry getServiceRegistry() {
        return this.serviceRegistry;
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        this.serverName = null;
        this.serviceRegistry = null;
        this.channel.shutdown();
        this.server.shutdown();

        try {
            this.channel.awaitTermination(1L, TimeUnit.MINUTES);
            this.server.awaitTermination(1L, TimeUnit.MINUTES);
        } catch (InterruptedException var5) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(var5);
        } finally {
            this.channel.shutdownNow();
            this.channel = null;
            this.server.shutdownNow();
            this.server = null;
        }

    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        this.serverName = UUID.randomUUID().toString();
        this.serviceRegistry = new MutableHandlerRegistry();
        InProcessServerBuilder serverBuilder = InProcessServerBuilder.forName(this.serverName).fallbackHandlerRegistry(this.serviceRegistry);
        if (this.useDirectExecutor) {
            serverBuilder.directExecutor();
        }

        this.server = serverBuilder.build().start();
        InProcessChannelBuilder channelBuilder = InProcessChannelBuilder.forName(this.serverName);
        if (this.useDirectExecutor) {
            channelBuilder.directExecutor();
        }

        this.channel = channelBuilder.build();
    }
}

