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

package com.automq.rocketmq.proxy.grpc;

import apache.rocketmq.common.v1.Code;
import apache.rocketmq.proxy.v1.ConsumerClientConnection;
import apache.rocketmq.proxy.v1.ConsumerClientConnectionReply;
import apache.rocketmq.proxy.v1.ConsumerClientConnectionRequest;
import apache.rocketmq.proxy.v1.ProducerClientConnection;
import apache.rocketmq.proxy.v1.ProducerClientConnectionReply;
import apache.rocketmq.proxy.v1.ProducerClientConnectionRequest;
import apache.rocketmq.proxy.v1.ProxyServiceGrpc;
import apache.rocketmq.proxy.v1.QueueStats;
import apache.rocketmq.proxy.v1.RelayReply;
import apache.rocketmq.proxy.v1.RelayRequest;
import apache.rocketmq.proxy.v1.ResetConsumeOffsetByTimestampRequest;
import apache.rocketmq.proxy.v1.ResetConsumeOffsetReply;
import apache.rocketmq.proxy.v1.ResetConsumeOffsetRequest;
import apache.rocketmq.proxy.v1.Status;
import apache.rocketmq.proxy.v1.TopicStatsReply;
import apache.rocketmq.proxy.v1.TopicStatsRequest;
import com.automq.rocketmq.common.model.generated.FlatMessage;
import com.automq.rocketmq.proxy.service.ExtendMessageService;
import com.automq.rocketmq.store.api.MessageStore;
import com.automq.rocketmq.store.model.StoreContext;
import com.google.protobuf.TextFormat;
import io.grpc.stub.StreamObserver;
import io.netty.channel.Channel;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.broker.client.ProducerManager;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcClientChannel;
import org.apache.rocketmq.proxy.processor.channel.ChannelProtocolType;
import org.slf4j.Logger;

public class ProxyServiceImpl extends ProxyServiceGrpc.ProxyServiceImplBase {
    private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(ProxyServiceImpl.class);

    private final MessageStore messageStore;

    private final ExtendMessageService messageService;

    private final ProducerManager producerManager;
    private final ConsumerManager consumerManager;

    public ProxyServiceImpl(MessageStore messageStore, ExtendMessageService messageService,
        ProducerManager producerManager, ConsumerManager consumerManager) {
        this.messageStore = messageStore;
        this.messageService = messageService;
        this.producerManager = producerManager;
        this.consumerManager = consumerManager;
    }

    @Override
    public void resetConsumeOffset(ResetConsumeOffsetRequest request,
        StreamObserver<ResetConsumeOffsetReply> responseObserver) {
        LOGGER.info("Reset consume offset request received: {}", TextFormat.shortDebugString(request));
        messageService.resetConsumeOffset(request.getTopic(), request.getQueueId(), request.getGroup(), request.getNewConsumeOffset())
            .whenComplete((v, e) -> {
                if (e != null) {
                    responseObserver.onError(e);
                    return;
                }
                ResetConsumeOffsetReply reply = ResetConsumeOffsetReply.newBuilder()
                    .setStatus(Status
                        .newBuilder()
                        .setCode(Code.OK)
                        .build())
                    .build();
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
            });
    }

    @Override
    public void resetConsumeOffsetByTimestamp(ResetConsumeOffsetByTimestampRequest request,
        StreamObserver<ResetConsumeOffsetReply> responseObserver) {
        LOGGER.info("Reset consume offset by timestamp request received: {}", TextFormat.shortDebugString(request));
        messageService.resetConsumeOffsetByTimestamp(request.getTopic(), request.getQueueId(), request.getGroup(), request.getTimestamp())
            .whenComplete((v, e) -> {
                if (e != null) {
                    responseObserver.onError(e);
                    return;
                }
                ResetConsumeOffsetReply reply = ResetConsumeOffsetReply.newBuilder()
                    .setStatus(Status
                        .newBuilder()
                        .setCode(Code.OK)
                        .build())
                    .build();
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
            });
    }

    @Override
    public void topicStats(TopicStatsRequest request, StreamObserver<TopicStatsReply> responseObserver) {
        messageService.getTopicStats(request.getTopic(), request.getQueueId(), request.getGroup())
            .whenComplete((pair, e) -> {
                if (e != null) {
                    responseObserver.onError(e);
                    return;
                }

                Long topicId = pair.getLeft();
                List<QueueStats> queueStatsList = pair.getRight();
                TopicStatsReply reply = TopicStatsReply.newBuilder()
                    .setStatus(Status
                        .newBuilder()
                        .setCode(Code.OK)
                        .build())
                    .setId(topicId)
                    .setName(request.getTopic())
                    .addAllQueueStats(queueStatsList)
                    .build();

                responseObserver.onNext(reply);
                responseObserver.onCompleted();
            });
    }

    @Override
    public void producerClientConnection(ProducerClientConnectionRequest request,
        StreamObserver<ProducerClientConnectionReply> responseObserver) {
        ConcurrentHashMap<Channel, ClientChannelInfo> map = producerManager.getGroupChannelTable().get(request.getGroup());
        if (map == null) {
            responseObserver.onNext(ProducerClientConnectionReply.newBuilder()
                .setStatus(Status
                    .newBuilder()
                    .setCode(Code.BAD_REQUEST)
                    .setMessage("Producer group not found: " + request.getGroup())
                    .build())
                .build());
            responseObserver.onCompleted();
            return;
        }
        ProducerClientConnectionReply.Builder builder = ProducerClientConnectionReply.newBuilder();
        for (ClientChannelInfo info : map.values()) {
            String protocolType = ChannelProtocolType.REMOTING.name();
            if (info.getChannel() instanceof GrpcClientChannel) {
                protocolType = ChannelProtocolType.GRPC_V2.name();
            }
            builder.addConnection(ProducerClientConnection.newBuilder()
                .setClientId(info.getClientId())
                .setProtocol(protocolType)
                .setAddress(NetworkUtil.socketAddress2String(info.getChannel().remoteAddress()))
                .setLanguage(info.getLanguage().name())
                .setVersion(MQVersion.getVersionDesc(info.getVersion()))
                .setLastUpdateTime(info.getLastUpdateTimestamp())
                .build());
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void consumerClientConnection(ConsumerClientConnectionRequest request,
        StreamObserver<ConsumerClientConnectionReply> responseObserver) {
        ConsumerGroupInfo groupInfo = consumerManager.getConsumerGroupInfo(request.getGroup(), true);
        if (groupInfo == null) {
            responseObserver.onNext(ConsumerClientConnectionReply.newBuilder()
                .setStatus(Status
                    .newBuilder()
                    .setCode(Code.BAD_REQUEST)
                    .setMessage("Consumer group not found: " + request.getGroup())
                    .build())
                .build());
            responseObserver.onCompleted();
            return;
        }
        ConsumerClientConnectionReply.Builder builder = ConsumerClientConnectionReply.newBuilder();
        for (ClientChannelInfo info : groupInfo.getChannelInfoTable().values()) {
            String protocolType = ChannelProtocolType.REMOTING.name();
            if (info.getChannel() instanceof GrpcClientChannel) {
                protocolType = ChannelProtocolType.GRPC_V2.name();
            }
            builder.addConnection(ConsumerClientConnection.newBuilder()
                .setClientId(info.getClientId())
                .setProtocol(protocolType)
                .setAddress(NetworkUtil.socketAddress2String(info.getChannel().remoteAddress()))
                .setLanguage(info.getLanguage().name())
                .setVersion(MQVersion.getVersionDesc(info.getVersion()))
                .setLastUpdateTime(info.getLastUpdateTimestamp())
                .build());
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void relay(RelayRequest request, StreamObserver<RelayReply> responseObserver) {
        switch (request.getCommandCase()) {
            case PUT_MESSAGE_COMMAND -> {
                ByteBuffer buffer = request.getPutMessageCommand().getFlatMessage().asReadOnlyByteBuffer();
                FlatMessage flatMessage = FlatMessage.getRootAsFlatMessage(buffer);
                messageStore.put(StoreContext.EMPTY, flatMessage)
                    .whenComplete((v, e) -> {
                        if (e != null) {
                            responseObserver.onError(e);
                            return;
                        }
                        responseObserver.onNext(RelayReply.newBuilder()
                            .setStatus(Status
                                .newBuilder()
                                .setCode(Code.OK)
                                .build())
                            .build());
                        responseObserver.onCompleted();
                    });
            }
            default -> {
                responseObserver.onNext(RelayReply.newBuilder()
                    .setStatus(Status
                        .newBuilder()
                        .setCode(Code.BAD_REQUEST)
                        .setMessage("Unsupported command: " + request.getCommandCase())
                        .build())
                    .build());
                responseObserver.onCompleted();
            }
        }
    }
}
