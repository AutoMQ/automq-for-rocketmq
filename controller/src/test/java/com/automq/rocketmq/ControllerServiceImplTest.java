package com.automq.rocketmq;

import apache.rocketmq.controller.v1.BrokerHeartbeatReply;
import apache.rocketmq.controller.v1.BrokerHeartbeatRequest;
import apache.rocketmq.controller.v1.Code;
import apache.rocketmq.controller.v1.ControllerServiceGrpc;
import io.grpc.*;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class ControllerServiceImplTest {

    @Test
    public void testHeartbeatGrpc() throws IOException, InterruptedException {
        ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl());
        testServer.start();

        int port = testServer.getPort();
        ManagedChannel channel = Grpc.newChannelBuilderForAddress("localhost", port, InsecureChannelCredentials.create()).build();
        ControllerServiceGrpc.ControllerServiceBlockingStub blockingStub = ControllerServiceGrpc.newBlockingStub(channel);
        BrokerHeartbeatRequest request = BrokerHeartbeatRequest.newBuilder().setBrokerId(1).setBrokerEpoch(1).build();
        BrokerHeartbeatReply reply = blockingStub.processBrokerHeartbeat(request);
        assertEquals(Code.OK, reply.getStatus().getCode());
        testServer.stop();
    }

}