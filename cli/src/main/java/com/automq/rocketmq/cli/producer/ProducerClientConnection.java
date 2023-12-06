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

package com.automq.rocketmq.cli.producer;

import apache.rocketmq.proxy.v1.ProducerClientConnectionRequest;
import com.automq.rocketmq.cli.CliClientConfig;
import com.automq.rocketmq.cli.MQAdmin;
import com.automq.rocketmq.proxy.grpc.client.GrpcProxyClient;
import de.vandermeer.asciitable.AT_Row;
import de.vandermeer.asciitable.AsciiTable;
import de.vandermeer.asciitable.CWC_LongestLine;
import de.vandermeer.skb.interfaces.transformers.textformat.TextAlignment;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.IntStream;
import picocli.CommandLine;

@CommandLine.Command(name = "producerClientConnection", mixinStandardHelpOptions = true, showDefaultValues = true)
public class ProducerClientConnection implements Callable<Void> {
    @CommandLine.ParentCommand
    MQAdmin mqAdmin;

    @CommandLine.Option(names = {"-g", "--groupName"}, description = "Group name", required = true)
    String groupName;

    @Override
    public Void call() throws Exception {
        GrpcProxyClient proxyClient = new GrpcProxyClient(new CliClientConfig());

        List<apache.rocketmq.proxy.v1.ProducerClientConnection> connections = proxyClient.producerClientConnection(mqAdmin.getEndpoint(), ProducerClientConnectionRequest.newBuilder().setProductionGroup(groupName).build()).get();

        AsciiTable groupTable = new AsciiTable();
        groupTable.addRule();
        AT_Row row = groupTable.addRow("CLIENT ID", "PROTOCOL", "VERSION", "ADDRESS", "LANGUAGE");
        centralize(row);
        groupTable.addRule();

        for (apache.rocketmq.proxy.v1.ProducerClientConnection connection : connections) {
            row = groupTable.addRow(connection.getClientId(), connection.getProtocol(), connection.getVersion(), connection.getAddress(), connection.getLanguage());
            centralize(row);
            groupTable.addRule();
        }

        CWC_LongestLine cwc = new CWC_LongestLine();
        IntStream.range(0, row.getCells().size()).forEach((i) -> {
            cwc.add(10, 0);
        });
        groupTable.getRenderer().setCWC(cwc);

        String render = groupTable.render();
        System.out.println(render);
        return null;
    }

    private void centralize(AT_Row row) {
        row.getCells().forEach((cell -> cell.getContext().setTextAlignment(TextAlignment.CENTER)));
    }
}
