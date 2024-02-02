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

package com.automq.rocketmq.cli.consumer;

import apache.rocketmq.proxy.v1.ConsumerClientConnectionRequest;
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

@CommandLine.Command(name = "consumerClientConnection", mixinStandardHelpOptions = true, showDefaultValues = true)
public class ConsumerClientConnection implements Callable<Void> {
    @CommandLine.ParentCommand
    MQAdmin mqAdmin;

    @CommandLine.Option(names = {"-g", "--groupName"}, description = "Group name", required = true)
    String groupName;

    @Override
    public Void call() throws Exception {
        GrpcProxyClient proxyClient = new GrpcProxyClient(new CliClientConfig());

        List<apache.rocketmq.proxy.v1.ConsumerClientConnection> connections = proxyClient.consumerClientConnection(mqAdmin.getEndpoint(), ConsumerClientConnectionRequest.newBuilder().setGroup(groupName).build()).get();

        AsciiTable groupTable = new AsciiTable();
        groupTable.addRule();
        AT_Row row = groupTable.addRow("CLIENT ID", "PROTOCOL", "VERSION", "ADDRESS", "LANGUAGE");
        centralize(row);
        groupTable.addRule();

        for (apache.rocketmq.proxy.v1.ConsumerClientConnection connection : connections) {
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
