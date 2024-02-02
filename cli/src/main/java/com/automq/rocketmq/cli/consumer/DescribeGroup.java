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

import apache.rocketmq.controller.v1.ConsumerGroup;
import com.automq.rocketmq.cli.CliClientConfig;
import com.automq.rocketmq.cli.MQAdmin;
import com.automq.rocketmq.controller.ControllerClient;
import com.automq.rocketmq.controller.client.GrpcControllerClient;
import de.vandermeer.asciitable.AT_Row;
import de.vandermeer.asciitable.AsciiTable;
import de.vandermeer.asciitable.CWC_LongestLine;
import de.vandermeer.skb.interfaces.transformers.textformat.TextAlignment;
import java.util.concurrent.Callable;
import java.util.stream.IntStream;
import picocli.CommandLine;

@CommandLine.Command(name = "describeGroup", mixinStandardHelpOptions = true, showDefaultValues = true)
public class DescribeGroup implements Callable<Void> {

    @CommandLine.Option(names = {"-g", "--groupName"}, description = "Group Name", required = true)
    String groupName;

    @CommandLine.ParentCommand
    MQAdmin mqAdmin;

    @Override
    public Void call() throws Exception {
        try (ControllerClient client = new GrpcControllerClient(new CliClientConfig())) {
            ConsumerGroup group = client.describeGroup(mqAdmin.getEndpoint(), groupName)
                .join();
            if (null == group) {
                System.err.printf("Group '%s' is not found%n%n", groupName);
                return null;
            }

            AsciiTable groupTable = new AsciiTable();
            groupTable.addRule();
            AT_Row row = groupTable.addRow("NAME", "ID", "TYPE", "SUBSCRIPTION MODE", "MAX DELIVERY ATTEMPT", "DEAD LETTER TOPIC ID");
            centralize(row);
            groupTable.addRule();
            String groupType;
            switch (group.getGroupType()) {
                case GROUP_TYPE_STANDARD -> groupType = "Standard";
                case GROUP_TYPE_FIFO -> groupType = "FIFO";
                default -> groupType = "Unknown";
            }

            String subMode;
            switch (group.getSubMode()) {
                case SUB_MODE_POP -> subMode = "POP";
                case SUB_MODE_PULL -> subMode = "PULL";
                default -> subMode = "UNSPECIFIED";
            }

            row = groupTable.addRow(group.getName(), group.getGroupId(), groupType, subMode,
                group.getMaxDeliveryAttempt(), group.getDeadLetterTopicId());
            centralize(row);
            groupTable.addRule();

            CWC_LongestLine cwc = new CWC_LongestLine();
            IntStream.range(0, row.getCells().size()).forEach((i) -> {
                cwc.add(10, 0);
            });
            groupTable.getRenderer().setCWC(cwc);

            String render = groupTable.render();
            System.out.println(render);
        }
        return null;
    }

    private void centralize(AT_Row row) {
        row.getCells().forEach((cell -> cell.getContext().setTextAlignment(TextAlignment.CENTER)));
    }
}
