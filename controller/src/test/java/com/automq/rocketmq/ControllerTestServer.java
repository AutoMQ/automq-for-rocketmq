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
