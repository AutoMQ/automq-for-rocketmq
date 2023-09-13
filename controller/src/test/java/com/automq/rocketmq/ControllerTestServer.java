package com.automq.rocketmq;

import io.grpc.BindableService;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ControllerTestServer {
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
}
