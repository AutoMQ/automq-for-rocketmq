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

import apache.rocketmq.controller.v1.BrokerHeartbeatReply;
import apache.rocketmq.controller.v1.BrokerHeartbeatRequest;
import apache.rocketmq.controller.v1.BrokerRegistrationReply;
import apache.rocketmq.controller.v1.BrokerRegistrationRequest;
import apache.rocketmq.controller.v1.Code;
import apache.rocketmq.controller.v1.ControllerServiceGrpc;
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

import static org.junit.Assert.assertEquals;

public class ControllerServiceImplTest extends DatabaseTestBase {

    @Test
    public void testRegisterBroker() throws IOException {
        ControllerConfig config = Mockito.mock(ControllerConfig.class);
        Mockito.when(config.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(config.brokerId()).thenReturn(1);
        Mockito.when(config.leaseLifeSpanInSecs()).thenReturn(1);

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(getSessionFactory(), config)) {
            metadataStore.start();
            ControllerServiceImpl svc = new ControllerServiceImpl(metadataStore);
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);
            BrokerRegistrationRequest request = BrokerRegistrationRequest.newBuilder()
                .setBrokerName("broker-name")
                .setAddress("localhost:1234")
                .setInstanceId("i-reg-broker")
                .build();

            StreamObserver<BrokerRegistrationReply> observer = new StreamObserver() {
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

            svc.registerBroker(request, observer);
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
        BrokerHeartbeatRequest request = BrokerHeartbeatRequest.newBuilder().setId(1).setTerm(1).build();
        BrokerHeartbeatReply reply = blockingStub.processBrokerHeartbeat(request);
        assertEquals(Code.OK, reply.getStatus().getCode());
        channel.shutdownNow();
        testServer.stop();
    }

}