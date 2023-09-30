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

package com.automq.rocketmq.proxy.service;

import apache.rocketmq.controller.v1.MessageQueueAssignment;
import apache.rocketmq.controller.v1.Topic;
import com.automq.rocketmq.metadata.ProxyMetadataService;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.proxy.common.Address;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;
import org.apache.rocketmq.proxy.service.route.MessageQueueView;
import org.apache.rocketmq.proxy.service.route.ProxyTopicRouteData;
import org.apache.rocketmq.proxy.service.route.TopicRouteService;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.checkerframework.checker.units.qual.A;

public class TopicRouteServiceImpl extends TopicRouteService {
    public static final String VIRTUAL_CLUSTER_NAME = "DefaultCluster";
    private final ProxyMetadataService metadataService;

    public TopicRouteServiceImpl(ProxyMetadataService metadataService) {
        // We don't need MQClientAPIFactory anymore, so just pass a null.
        super(null);
        this.metadataService = metadataService;
    }

    @Override
    protected void init() {
        this.appendShutdown(this.scheduledExecutorService::shutdown);
    }

    @Override
    public MessageQueueView getAllMessageQueueView(ProxyContext ctx, String topicName) throws Exception {
        Topic topic = metadataService.topicOf(topicName).join();
        TopicRouteData topicRouteData = new TopicRouteData();
        // Build queue data
        topicRouteData.setQueueDatas(queueDataFrom(topic.getAssignmentsList(), topicName));
        // Build broker data
        topicRouteData.setBrokerDatas(brokerDataFrom(topic.getAssignmentsList(), topicName));
        return new MessageQueueView(topicName, topicRouteData);
    }

    @Override
    public MessageQueueView getCurrentMessageQueueView(ProxyContext ctx, String topicName) throws Exception {
        List<MessageQueueAssignment> assignments = metadataService.queueAssignmentsOf(topicName).join();
        TopicRouteData topicRouteData = new TopicRouteData();
        // Build queue data
        topicRouteData.setQueueDatas(queueDataFrom(assignments, topicName));
        topicRouteData.setBrokerDatas(brokerDataFrom(assignments, topicName));
        return new MessageQueueView(topicName, topicRouteData);
    }

    @Override
    public ProxyTopicRouteData getTopicRouteForProxy(ProxyContext ctx, List<Address> requestHostAndPortList,
        String topicName) throws Exception {
        MessageQueueView messageQueueView = getAllMessageQueueView(ctx, topicName);
        TopicRouteData topicRouteData = messageQueueView.getTopicRouteData();

        ProxyTopicRouteData proxyTopicRouteData = new ProxyTopicRouteData();
        proxyTopicRouteData.setQueueDatas(topicRouteData.getQueueDatas());

        for (BrokerData brokerData : topicRouteData.getBrokerDatas()) {
            ProxyTopicRouteData.ProxyBrokerData proxyBrokerData = new ProxyTopicRouteData.ProxyBrokerData();
            proxyBrokerData.setCluster(brokerData.getCluster());
            proxyBrokerData.setBrokerName(brokerData.getBrokerName());
            for (Long brokerId : brokerData.getBrokerAddrs().keySet()) {
                String brokerAddr = brokerData.getBrokerAddrs().get(brokerId);
                HostAndPort brokerHostAndPort = HostAndPort.fromString(brokerAddr);
                proxyBrokerData.getBrokerAddrs().put(brokerId, Lists.newArrayList(new Address(Address.AddressScheme.IPv4, brokerHostAndPort)));
            }
            proxyTopicRouteData.getBrokerDatas().add(proxyBrokerData);
        }

        return proxyTopicRouteData;
    }

    @Override
    public String getBrokerAddr(ProxyContext ctx, String brokerName) throws Exception {
        throw new UnsupportedOperationException("No need to implement this method.");
    }

    @Override
    public AddressableMessageQueue buildAddressableMessageQueue(ProxyContext ctx,
        MessageQueue messageQueue) throws Exception {
        // In local mode, we don't need to know the broker address.
        return new AddressableMessageQueue(messageQueue, null);
    }

    /**
     * Get the virtual broker name for a given topic queue.
     *
     * @param topicName topic name
     * @param queueId   queue id
     * @return virtual broker name
     */
    private String virtualBrokerName(String topicName, int queueId) {
        return topicName + "%" + queueId;
    }

    private List<QueueData> queueDataFrom(List<MessageQueueAssignment> assignments, String topicName) {
        List<QueueData> queueDatas = new ArrayList<>();
        assignments.forEach(assignment -> {
            QueueData queueData = new QueueData();
            queueData.setBrokerName(virtualBrokerName(topicName, assignment.getQueue().getQueueId()));
            queueData.setReadQueueNums(1);
            queueData.setWriteQueueNums(1);
            queueData.setPerm(PermName.PERM_READ | PermName.PERM_WRITE);
            queueDatas.add(queueData);
        });
        return queueDatas;
    }

    private List<BrokerData> brokerDataFrom(List<MessageQueueAssignment> assignments, String topicName) {
        // Retrieve the broker list from the topic metadata.
        Map<Integer, String> addressMap = new HashMap<>();
        Set<Integer> brokerIdSet = assignments.stream().map(MessageQueueAssignment::getBrokerId).collect(Collectors.toSet());

        CompletableFuture<String>[] cfs = new CompletableFuture[brokerIdSet.size()];
        int i = 0;
        for (Integer brokerId : brokerIdSet) {
            cfs[i++] = metadataService.addressOf(brokerId).thenApply(address -> {
                addressMap.put(brokerId, address);
                return address;
            });
        }
        CompletableFuture.allOf(cfs).join();

        List<BrokerData> brokerDatas = new ArrayList<>();
        addressMap.forEach((brokerId, address) -> {
            BrokerData brokerData = new BrokerData();
            brokerData.setBrokerName(virtualBrokerName(topicName, brokerId));
            brokerData.setCluster(VIRTUAL_CLUSTER_NAME);
            brokerData.setBrokerAddrs(new HashMap<>() {{
                put(0L, address);
            }});
            brokerDatas.add(brokerData);
        });
        return brokerDatas;
    }
}
