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

package com.automq.rocketmq.controller.metadata;

import apache.rocketmq.controller.v1.Code;
import com.automq.rocketmq.controller.ControllerServiceImpl;
import com.automq.rocketmq.controller.ControllerTestServer;
import com.automq.rocketmq.controller.exception.ControllerException;
import com.automq.rocketmq.controller.metadata.database.dao.Node;
import java.io.IOException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

class GrpcControllerClientTest {

    @Test
    public void testRegisterBroker() throws IOException, ControllerException {
        String name = "broker-name";
        String address = "localhost:1234";
        String instanceId = "i-ctrl";
        MetadataStore metadataStore = Mockito.mock(MetadataStore.class);
        Node node = new Node();
        node.setId(1);
        node.setEpoch(1);
        Mockito.when(metadataStore.registerBrokerNode(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
            ArgumentMatchers.anyString())).thenReturn(node);
        ControllerServiceImpl svc = new ControllerServiceImpl(metadataStore);
        try (ControllerTestServer testServer = new ControllerTestServer(0, svc)) {
            testServer.start();
            int port = testServer.getPort();
            ControllerClient client = new GrpcControllerClient();
            Node result = client.registerBroker(String.format("localhost:%d", port), name, address, instanceId);
            Assertions.assertEquals(1, result.getId());
            Assertions.assertEquals(1, result.getEpoch());
            Assertions.assertEquals(name, result.getName());
            Assertions.assertEquals(address, result.getAddress());
            Assertions.assertEquals(instanceId, result.getInstanceId());
        }
    }

    @Test
    public void testRegisterBroker_badTarget() throws IOException, ControllerException {
        String name = "broker-name";
        String address = "localhost:1234";
        String instanceId = "i-ctrl";
        MetadataStore metadataStore = Mockito.mock(MetadataStore.class);
        Node node = new Node();
        node.setId(1);
        node.setEpoch(1);
        Mockito.when(metadataStore.registerBrokerNode(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
            ArgumentMatchers.anyString())).thenReturn(node);
        ControllerClient client = new GrpcControllerClient();
        Assertions.assertThrows(ControllerException.class,
            () -> client.registerBroker(null, name, address, instanceId));
    }

    @Test
    public void testRegisterBroker_leaderFailure() throws IOException, ControllerException {
        String name = "broker-name";
        String address = "localhost:1234";
        String instanceId = "i-ctrl";
        MetadataStore metadataStore = Mockito.mock(MetadataStore.class);
        Node node = new Node();
        node.setId(1);
        node.setEpoch(1);
        Mockito.when(metadataStore.registerBrokerNode(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
            ArgumentMatchers.anyString())).thenThrow(new ControllerException(Code.MOCK_FAILURE_VALUE));
        ControllerServiceImpl svc = new ControllerServiceImpl(metadataStore);
        try (ControllerTestServer testServer = new ControllerTestServer(0, svc)) {
            testServer.start();
            int port = testServer.getPort();
            ControllerClient client = new GrpcControllerClient();
            Assertions.assertThrows(ControllerException.class,
                () -> client.registerBroker(String.format("localhost:%d", port), name, address, instanceId));

        }
    }
}