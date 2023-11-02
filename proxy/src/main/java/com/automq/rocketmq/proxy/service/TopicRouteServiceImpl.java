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

import apache.rocketmq.controller.v1.AcceptTypes;
import apache.rocketmq.common.v1.Code;
import apache.rocketmq.controller.v1.CreateTopicRequest;
import apache.rocketmq.controller.v1.MessageQueueAssignment;
import apache.rocketmq.controller.v1.MessageType;
import apache.rocketmq.controller.v1.Topic;
import com.automq.rocketmq.common.config.BrokerConfig;
import com.automq.rocketmq.common.exception.ControllerException;
import com.automq.rocketmq.metadata.api.ProxyMetadataService;
import com.automq.rocketmq.proxy.model.VirtualQueue;
import com.automq.rocketmq.proxy.remoting.RemotingUtil;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.proxy.common.Address;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.utils.ExceptionUtils;
import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;
import org.apache.rocketmq.proxy.service.route.MessageQueueView;
import org.apache.rocketmq.proxy.service.route.ProxyTopicRouteData;
import org.apache.rocketmq.proxy.service.route.TopicRouteService;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;

public class TopicRouteServiceImpl extends TopicRouteService {
    public static final String VIRTUAL_CLUSTER_NAME = "DefaultCluster";
    private final ProxyMetadataService metadataService;
    private final BrokerConfig brokerConfig;

    public TopicRouteServiceImpl(BrokerConfig config, ProxyMetadataService metadataService) {
        // We don't need MQClientAPIFactory anymore, so just pass a null.
        super(null);
        this.metadataService = metadataService;
        this.brokerConfig = config;

        // The parent class has created a scheduledExecutorService, but we don't need it, just destroy it.
        this.scheduledExecutorService.shutdown();
    }

    @Override
    protected void init() {
        // Do nothing.
        // Just disable the behavior of the parent class.
    }

    @Override
    public MessageQueueView getAllMessageQueueView(ProxyContext ctx, String topicName) {
        return new MessageQueueView(topicName, routeDataFrom(assignmentsOf(ctx, topicName, QueueFilter.ALL)));
    }

    @Override
    public MessageQueueView getCurrentMessageQueueView(ProxyContext ctx, String topicName) {
        // Only return the MessageQueueAssignment that is assigned to the current broker.
        return new MessageQueueView(topicName, routeDataFrom(assignmentsOf(ctx, topicName, nodeId -> nodeId == brokerConfig.nodeId())));
    }

    @Override
    public ProxyTopicRouteData getTopicRouteForProxy(ProxyContext ctx, List<Address> requestHostAndPortList,
        String topicName) {
        MessageQueueView messageQueueView = getAllMessageQueueView(ctx, topicName);
        TopicRouteData topicRouteData = messageQueueView.getTopicRouteData();

        ProxyTopicRouteData proxyTopicRouteData = new ProxyTopicRouteData();
        proxyTopicRouteData.setQueueDatas(topicRouteData.getQueueDatas());
        boolean isGrpc = !RemotingUtil.isRemotingProtocol(ctx);

        for (BrokerData brokerData : topicRouteData.getBrokerDatas()) {
            ProxyTopicRouteData.ProxyBrokerData proxyBrokerData = new ProxyTopicRouteData.ProxyBrokerData();
            proxyBrokerData.setCluster(brokerData.getCluster());
            proxyBrokerData.setBrokerName(brokerData.getBrokerName());
            for (Long brokerId : brokerData.getBrokerAddrs().keySet()) {
                String brokerAddr = brokerData.getBrokerAddrs().get(brokerId);
                HostAndPort brokerHostAndPort = HostAndPort.fromString(brokerAddr);
                if (isGrpc) {
                    // This is the default behavior of S3RocketMQ broker, that the gRPC port is always 1 more than the remoting port.
                    brokerHostAndPort = HostAndPort.fromParts(brokerHostAndPort.getHost(), brokerHostAndPort.getPort() + 1);
                }
                proxyBrokerData.getBrokerAddrs().put(brokerId, Lists.newArrayList(new Address(Address.AddressScheme.IPv4, brokerHostAndPort)));
            }
            proxyTopicRouteData.getBrokerDatas().add(proxyBrokerData);
        }

        return proxyTopicRouteData;
    }

    @Override
    public String getBrokerAddr(ProxyContext ctx, String brokerName) {
        throw new UnsupportedOperationException("No need to implement this method.");
    }

    @Override
    public AddressableMessageQueue buildAddressableMessageQueue(ProxyContext ctx,
        MessageQueue messageQueue) {
        return new AddressableMessageQueue(messageQueue, null);
    }

    private TopicRouteData routeDataFrom(List<MessageQueueAssignment> assignments) {
        TopicRouteData topicRouteData = new TopicRouteData();
        List<QueueData> queueDataList = new ArrayList<>();
        List<BrokerData> brokerDataList = new ArrayList<>();

        Map<Integer, String> addressMap = new HashMap<>();
        Set<Integer> nodeIdSet = assignments.stream().map(MessageQueueAssignment::getNodeId).collect(Collectors.toSet());

        @SuppressWarnings("unchecked")
        CompletableFuture<String>[] cfs = new CompletableFuture[nodeIdSet.size()];
        int i = 0;
        for (Integer nodeId : nodeIdSet) {
            cfs[i++] = metadataService.addressOf(nodeId).thenApply(address -> {
                addressMap.put(nodeId, address);
                return address;
            });
        }
        CompletableFuture.allOf(cfs).join();

        assignments.forEach(assignment -> {
            VirtualQueue virtualQueue = new VirtualQueue(assignment.getQueue().getTopicId(), assignment.getQueue().getQueueId());
            QueueData queueData = new QueueData();
            queueData.setBrokerName(virtualQueue.brokerName());
            queueData.setReadQueueNums(1);
            queueData.setWriteQueueNums(1);
            queueData.setPerm(PermName.PERM_READ | PermName.PERM_WRITE);
            queueDataList.add(queueData);

            // Each MessageQueue has a virtual broker binding to it.
            BrokerData brokerData = new BrokerData();
            brokerData.setBrokerName(virtualQueue.brokerName());
            brokerData.setCluster(VIRTUAL_CLUSTER_NAME);
            HashMap<Long, String> brokerAddrs = new HashMap<>();
            brokerAddrs.put(0L, addressMap.get(assignment.getNodeId()));
            brokerData.setBrokerAddrs(brokerAddrs);
            brokerDataList.add(brokerData);
        });

        topicRouteData.setBrokerDatas(brokerDataList);
        topicRouteData.setQueueDatas(queueDataList);

        return topicRouteData;
    }

    private List<MessageQueueAssignment> assignmentsOf(ProxyContext ctx, String topicName, QueueFilter filter) {
        Topic topic = metadataService.topicOf(topicName)
            .exceptionallyCompose(ex -> {
                if (ExceptionUtils.getRealException(ex) instanceof ControllerException controllerException) {
                    // If pull retry topic does not exist.
                    boolean isRemoting = RemotingUtil.isRemotingProtocol(ctx);
                    if (controllerException.getErrorCode() == Code.NOT_FOUND.getNumber()
                        && isRemoting && topicName.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        CreateTopicRequest request = CreateTopicRequest.newBuilder()
                            .setTopic(topicName)
                            .setCount(1)
                            .setAcceptTypes(AcceptTypes.newBuilder().addTypes(MessageType.DELAY).build())
                            .setRetentionHours((int) Duration.ofDays(3).toHours())
                            .build();
                        return metadataService.createTopic(request);
                    }
                }
                return CompletableFuture.failedFuture(ex);
            })
            .join();
        List<MessageQueueAssignment> assignmentList = new ArrayList<>();
        topic.getAssignmentsList().forEach(assignment -> {
            if (filter.filter(assignment.getNodeId())) {
                assignmentList.add(assignment);
            }
        });
        // Convert OngoingMessageQueueReassignment to MessageQueueAssignment
        topic.getReassignmentsList().forEach(reassignment -> {
            if (filter.filter(reassignment.getDstNodeId())) {
                assignmentList.add(MessageQueueAssignment.newBuilder()
                    .setNodeId(reassignment.getDstNodeId())
                    .setQueue(reassignment.getQueue())
                    .build());
            }
        });

        return assignmentList;
    }

    // Define a filter for the MessageQueueAssignment class.
    interface QueueFilter {
        QueueFilter ALL = nodeId -> true;

        boolean filter(int nodeId);
    }
}
