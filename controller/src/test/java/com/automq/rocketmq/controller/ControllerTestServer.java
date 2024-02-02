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

package com.automq.rocketmq.controller;

import io.grpc.BindableService;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import java.io.Closeable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ControllerTestServer implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ControllerTestServer.class);

    private int port;

    private final Server server;

    public ControllerTestServer(int port, BindableService svc) {
        this.port = port;
        this.server = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
                .addService(svc)
                .build();
    }

    public void start() throws IOException {
        if (null != this.server) {
            this.server.start();
            this.port = this.server.getPort();
            LOGGER.info("TestServer is up and listening {}", port);
        }

    }

    public void stop() throws InterruptedException {
        if (null != this.server) {
            this.server.shutdownNow().awaitTermination();
        }
    }

    public int getPort() {
        return port;
    }

    @Override
    public void close() throws IOException {
        try {
            this.stop();
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }
}
