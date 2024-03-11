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

package com.automq.rocketmq.cli.consumer;

import apache.rocketmq.proxy.v1.ConsumerStatusReply;
import apache.rocketmq.proxy.v1.ConsumerStatusRequest;
import com.automq.rocketmq.cli.CliClientConfig;
import com.automq.rocketmq.cli.MQAdmin;
import com.automq.rocketmq.proxy.grpc.client.GrpcProxyClient;
import de.vandermeer.asciitable.AT_Row;
import de.vandermeer.asciitable.AsciiTable;
import de.vandermeer.asciitable.CWC_LongestLine;
import de.vandermeer.skb.interfaces.transformers.textformat.TextAlignment;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.body.ConsumeStatus;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.protocol.body.PopProcessQueueInfo;
import org.apache.rocketmq.remoting.protocol.body.ProcessQueueInfo;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import picocli.CommandLine;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.IntStream;

@CommandLine.Command(name = "consumerStatus", mixinStandardHelpOptions = true, showDefaultValues = true)
public class ConsumerStatus implements Callable<Void> {
    @CommandLine.ParentCommand
    MQAdmin mqAdmin;

    @CommandLine.Option(names = {"-g", "--groupName"}, description = "Group name", required = true)
    String groupName;
    @CommandLine.Option(names = {"-i", "--clientId"}, description = "clientId", required = false)
    String clientId;
    @CommandLine.Option(names = {"-s", "--jstack"}, description = "jstack", required = false)
    boolean jstackEnable;

    @Override
    public Void call() throws Exception {
        GrpcProxyClient proxyClient = new GrpcProxyClient(new CliClientConfig());

        ConsumerStatusReply consumerStatusReply
                = proxyClient.consumerStatus(mqAdmin.getEndpoint(),
                ConsumerStatusRequest.newBuilder().setGroup(groupName)
                        .setClientId(clientId).setJstackEnable(jstackEnable).build()).get();

        AsciiTable groupTable = new AsciiTable();
        groupTable.addStrongRule();
        groupTable.addRow("Consumer Group");
        groupTable.addRow("consumeType", "messageModel", "consumeFromWhere");
        groupTable.addRow(consumerStatusReply.getConsumeType(), consumerStatusReply.getMessageModel(), consumerStatusReply.getConsumeFromWhere());


        groupTable.addStrongRule();
        groupTable.addRow("Consumer Client");
        groupTable.addRow("CLIENT ID", "PROTOCOL", "VERSION", "ADDRESS", "LANGUAGE");
        groupTable.addRule();
        for (apache.rocketmq.proxy.v1.ConsumerClientConnection connection : consumerStatusReply.getConnectionList()) {
            groupTable.addRow(connection.getClientId(), connection.getProtocol(), connection.getVersion(), connection.getAddress(), connection.getLanguage());
            groupTable.addRule();
        }

        //Consumer Subscription
        final ConsumerRunningInfo consumerRunningInfo = ConsumerRunningInfo
                .decode(consumerStatusReply.getConsumerRunningInfo().toByteArray(), ConsumerRunningInfo.class);
        addGroupInfo(consumerRunningInfo,groupTable);

//        CWC_LongestLine cwc = new CWC_LongestLine();
//        IntStream.range(0, row.getCells().size()).forEach((i) -> {
//            cwc.add(10, 0);
//        });
//        groupTable.getRenderer().setCWC(cwc);

        String render = groupTable.render();
        System.out.println(render);
        return null;
    }

    private void addGroupInfo(ConsumerRunningInfo consumerRunningInfo,AsciiTable groupTable) {
        groupTable.addStrongRule();
        groupTable.addRow("Consumer Subscription");
        AT_Row row = groupTable.addRow("Topic", "PROTOCOL", "ClassFilter", "SubExpression");
        if (consumerRunningInfo.getSubscriptionSet() != null
                && !consumerRunningInfo.getSubscriptionSet().isEmpty()) {
            for (SubscriptionData subscriptionData : consumerRunningInfo.getSubscriptionSet()) {
                groupTable.addRow(subscriptionData.getTopic(), subscriptionData.isClassFilterMode(),
                        subscriptionData.getSubString());
                groupTable.addRule();
            }
        }

        groupTable.addStrongRule();
        groupTable.addRow("Consumer Offset");
        groupTable.addRow("Topic", "Broker Name", "QID", "Consumer Offset");
        if (consumerRunningInfo.getMqTable() != null
                && !consumerRunningInfo.getMqTable().entrySet().isEmpty()) {
            for (Map.Entry<MessageQueue, ProcessQueueInfo> entry : consumerRunningInfo.getMqTable().entrySet()) {
                groupTable.addRow(entry.getKey().getTopic(),
                        entry.getKey().getBrokerName(),
                        entry.getKey().getQueueId(),
                        entry.getValue().getCommitOffset());
                groupTable.addRule();
            }
        }

        groupTable.addStrongRule();
        groupTable.addRow("Consumer MQ Detail");
        groupTable.addRow("Topic", "Broker Name", "QID", "Consumer Offset");
        if (consumerRunningInfo.getMqTable() != null
                && !consumerRunningInfo.getMqTable().entrySet().isEmpty()) {
            for (Map.Entry<MessageQueue, ProcessQueueInfo> entry : consumerRunningInfo.getMqTable().entrySet()) {
                groupTable.addRow(entry.getKey().getTopic(),
                        entry.getKey().getBrokerName(),
                        entry.getKey().getQueueId(),
                        entry.getValue().toString());
                groupTable.addRule();
            }
        }

        groupTable.addStrongRule();
        groupTable.addRow("Consumer MQ Detail");
        groupTable.addRow("Topic", "Broker Name", "QID", "ProcessQueueInfo");
        if (consumerRunningInfo.getMqTable() != null
                && !consumerRunningInfo.getMqTable().entrySet().isEmpty()) {
            for (Map.Entry<MessageQueue, ProcessQueueInfo> entry : consumerRunningInfo.getMqTable().entrySet()) {
                groupTable.addRow(entry.getKey().getTopic(),
                        entry.getKey().getBrokerName(),
                        entry.getKey().getQueueId(),
                        entry.getValue().toString());
                groupTable.addRule();
            }
        }

        groupTable.addStrongRule();
        groupTable.addRow("Consumer Pop Detail");
        groupTable.addRow("Topic", "Broker Name", "QID", "ProcessQueueInfo");
        if (consumerRunningInfo.getMqTable() != null
                && !consumerRunningInfo.getMqTable().entrySet().isEmpty()) {
            for (Map.Entry<MessageQueue, PopProcessQueueInfo> entry : consumerRunningInfo.getMqPopTable().entrySet()) {
                groupTable.addRow(entry.getKey().getTopic(),
                        entry.getKey().getBrokerName(),
                        entry.getKey().getQueueId(),
                        entry.getValue().toString());
                groupTable.addRule();
            }
        }

        groupTable.addStrongRule();
        groupTable.addRow("Consumer RT&TPS");
        groupTable.addRow("Topic", "Pull RT", "Pull TPS", "Consume RT", "ConsumeOK TPS", "ConsumeFailed TPS", "ConsumeFailedMsgsInHour");
        if (consumerRunningInfo.getStatusTable() != null
                && !consumerRunningInfo.getMqTable().entrySet().isEmpty()) {
            for (Map.Entry<String, ConsumeStatus> entry : consumerRunningInfo.getStatusTable().entrySet()) {
                groupTable.addRow(entry.getKey(),
                        entry.getValue().getPullRT(),
                        entry.getValue().getPullTPS(),
                        entry.getValue().getConsumeRT(),
                        entry.getValue().getConsumeOKTPS(),
                        entry.getValue().getConsumeFailedTPS(),
                        entry.getValue().getConsumeFailedMsgs());
                groupTable.addRule();
            }
        }

        groupTable.addStrongRule();
        groupTable.addRow("User Consume Info");
        groupTable.addRow("Topic", "Pull RT", "Pull TPS", "Consume RT", "ConsumeOK TPS", "ConsumeFailed TPS", "ConsumeFailedMsgsInHour");
        if (consumerRunningInfo.getUserConsumerInfo() != null
                && !consumerRunningInfo.getUserConsumerInfo().entrySet().isEmpty()) {
            for (Map.Entry<String, String> entry : consumerRunningInfo.getUserConsumerInfo().entrySet()) {
                groupTable.addRow(entry.getKey(), entry.getValue());
                groupTable.addRule();
            }
        }

        groupTable.addStrongRule();
        groupTable.addRow("Consumer jstack");
        if (consumerRunningInfo.getJstack() != null &&
                !consumerRunningInfo.getJstack().isEmpty()) {
            groupTable.addRow(consumerRunningInfo.getJstack());
        }

    }

    private void centralize(AT_Row row) {
        row.setTextAlignment(TextAlignment.CENTER);
//        row.getCells().forEach((cell -> cell.getContext().setTextAlignment(TextAlignment.CENTER)));
    }
}
