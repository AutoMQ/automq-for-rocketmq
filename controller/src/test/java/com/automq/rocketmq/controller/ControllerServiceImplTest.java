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

package com.automq.rocketmq.controller;

import apache.rocketmq.controller.v1.Code;
import apache.rocketmq.controller.v1.ControllerServiceGrpc;
import apache.rocketmq.controller.v1.HeartbeatReply;
import apache.rocketmq.controller.v1.HeartbeatRequest;
import apache.rocketmq.controller.v1.NodeRegistrationReply;
import apache.rocketmq.controller.v1.NodeRegistrationRequest;
import com.automq.rocketmq.controller.metadata.ControllerClient;
import com.automq.rocketmq.controller.metadata.ControllerConfig;
import com.automq.rocketmq.controller.metadata.DatabaseTestBase;
import com.automq.rocketmq.controller.metadata.MetadataStore;
import com.automq.rocketmq.controller.metadata.database.DefaultMetadataStore;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ControllerServiceImplTest extends DatabaseTestBase {

    @Test
    public void testRegisterBroker() throws IOException {
        ControllerConfig config = Mockito.mock(ControllerConfig.class);
        Mockito.when(config.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(config.nodeId()).thenReturn(1);
        Mockito.when(config.leaseLifeSpanInSecs()).thenReturn(1);

        ControllerClient client = Mockito.mock(ControllerClient.class);

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            ControllerServiceImpl svc = new ControllerServiceImpl(metadataStore);
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);
            NodeRegistrationRequest request = NodeRegistrationRequest.newBuilder()
                .setBrokerName("broker-name")
                .setAddress("localhost:1234")
                .setInstanceId("i-reg-broker")
                .build();

            StreamObserver<NodeRegistrationReply> observer = new StreamObserver() {
                @Override
                public void onNext(Object value) {

                }

                @Override
                public void onError(Throwable t) {
                    Assertions.fail(t);
                }

                @Override
                public void onCompleted() {

                }
            };

            svc.registerNode(request, observer);

            // It should work if register the broker multiple times. The only side effect is epoch is incremented.
            svc.registerNode(request, observer);
        }
    }

    @Test
    public void testRegisterBroker_BadRequest() throws IOException {
        ControllerConfig config = Mockito.mock(ControllerConfig.class);
        Mockito.when(config.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(config.nodeId()).thenReturn(1);
        Mockito.when(config.leaseLifeSpanInSecs()).thenReturn(1);

        ControllerClient client = Mockito.mock(ControllerClient.class);

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            ControllerServiceImpl svc = new ControllerServiceImpl(metadataStore);
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);
            NodeRegistrationRequest request = NodeRegistrationRequest.newBuilder()
                .setBrokerName("")
                .setAddress("localhost:1234")
                .setInstanceId("i-reg-broker")
                .build();

            StreamObserver<NodeRegistrationReply> observer = new StreamObserver() {
                @Override
                public void onNext(Object value) {
                    Assertions.fail("Should have raised an exception");
                }

                @Override
                public void onError(Throwable t) {
                    // OK, it's expected.
                }

                @Override
                public void onCompleted() {
                    Assertions.fail("Should have raised an exception");
                }
            };
            svc.registerNode(request, observer);
        }
    }

    @Test
    public void testHeartbeatGrpc() throws IOException, InterruptedException {
        MetadataStore metadataStore = Mockito.mock(MetadataStore.class);
        ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore));
        testServer.start();

        int port = testServer.getPort();
        ManagedChannel channel = Grpc.newChannelBuilderForAddress("localhost", port, InsecureChannelCredentials.create()).build();
        ControllerServiceGrpc.ControllerServiceBlockingStub blockingStub = ControllerServiceGrpc.newBlockingStub(channel);
        HeartbeatRequest request = HeartbeatRequest.newBuilder().setId(1).setEpoch(1).build();
        HeartbeatReply reply = blockingStub.processHeartbeat(request);
        Assertions.assertEquals(Code.OK, reply.getStatus().getCode());
        channel.shutdownNow();
        testServer.stop();
    }

}