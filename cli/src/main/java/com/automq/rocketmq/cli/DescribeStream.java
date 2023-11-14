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

package com.automq.rocketmq.cli;

import apache.rocketmq.common.v1.Code;
import apache.rocketmq.controller.v1.DescribeStreamReply;
import apache.rocketmq.controller.v1.DescribeStreamRequest;
import com.automq.rocketmq.controller.ControllerClient;
import com.automq.rocketmq.controller.client.GrpcControllerClient;
import java.util.concurrent.Callable;
import picocli.CommandLine;

@CommandLine.Command(name = "describeStream", mixinStandardHelpOptions = true, showDefaultValues = true)
public class DescribeStream implements Callable<Void> {

    @CommandLine.ParentCommand
    MQAdmin mqAdmin;

    @CommandLine.Option(names = {"-i", "--streamId"}, description = "Stream ID", required = true)
    long streamId;

    @Override
    public Void call() throws Exception {
        try (ControllerClient client = new GrpcControllerClient(new CliClientConfig())) {
            DescribeStreamRequest request = DescribeStreamRequest.newBuilder()
                .setStreamId(streamId)
                .build();
            DescribeStreamReply reply = client.describeStream(mqAdmin.endpoint, request).join();
            if (reply.getStatus().getCode() == Code.OK) {
                ConsoleHelper.printStream(reply.getStream(), reply.getRangesList());
            } else {
                System.err.println(reply.getStatus().getMessage());
            }
        }
        return null;
    }
}
