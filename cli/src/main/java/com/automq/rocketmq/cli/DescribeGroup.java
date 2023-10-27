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

import apache.rocketmq.controller.v1.ConsumerGroup;
import com.automq.rocketmq.controller.metadata.ControllerClient;
import com.automq.rocketmq.controller.metadata.GrpcControllerClient;
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
        try (ControllerClient client = new GrpcControllerClient()) {
            ConsumerGroup group = client.describeGroup(mqAdmin.endpoint, groupName)
                .join();
            if (null == group) {
                System.err.printf("Group '%s' is not found%n%n", groupName);
                return null;
            }

            AsciiTable groupTable = new AsciiTable();
            groupTable.addRule();
            AT_Row row = groupTable.addRow("NAME", "ID", "TYPE", "MAX DELIVERY ATTEMPT", "DEAD LETTER TOPIC ID");
            centralize(row);
            groupTable.addRule();
            String groupType;
            switch (group.getGroupType()) {
                case GROUP_TYPE_STANDARD -> groupType = "Standard";
                case GROUP_TYPE_FIFO -> groupType = "FIFO";
                default -> groupType = "Unknown";
            }
            row = groupTable.addRow(group.getName(), group.getGroupId(), groupType, group.getMaxDeliveryAttempt(), group.getDeadLetterTopicId());
            centralize(row);
            groupTable.addRule();

            CWC_LongestLine cwc = new CWC_LongestLine();
            IntStream.range(0, 5).forEach((i) -> {
                cwc.add(10, 0);
            });
            groupTable.getRenderer().setCWC(cwc);

            String render = groupTable.render();
            System.out.println(render);
        }
        return null;
    }

    private void centralize(AT_Row row) {
        row.getCells().forEach((cell -> {
            cell.getContext().setTextAlignment(TextAlignment.CENTER);
        }));
    }
}
