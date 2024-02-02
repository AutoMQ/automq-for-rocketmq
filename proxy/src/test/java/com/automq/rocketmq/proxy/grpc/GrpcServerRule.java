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
    public void afterEach(ExtensionContext context) {
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

