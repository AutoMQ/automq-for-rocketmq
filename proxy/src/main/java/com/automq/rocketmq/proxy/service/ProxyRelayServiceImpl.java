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

package com.automq.rocketmq.proxy.service;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import com.automq.rocketmq.proxy.remoting.RemotingProtocolServer;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.utils.ExceptionUtils;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayResult;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.apache.rocketmq.proxy.service.relay.RelayData;
import org.apache.rocketmq.proxy.service.transaction.TransactionData;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerRunningInfoRequestHeader;

public class ProxyRelayServiceImpl implements ProxyRelayService {

    private static final long DEFAULT_MQ_CLIENT_TIMEOUT = Duration.ofSeconds(3).toMillis();
    private final ConsumerManager consumerManager;

    private final RemotingProtocolServer remotingProtocolServer;

    public ProxyRelayServiceImpl(ConsumerManager consumerManager, RemotingProtocolServer remotingProtocolServer) {
        this.consumerManager = consumerManager;
        this.remotingProtocolServer = remotingProtocolServer;
    }

    @Override
    public CompletableFuture<ProxyRelayResult<ConsumerRunningInfo>> processGetConsumerRunningInfo(ProxyContext context,
                                                                                                  RemotingCommand command, GetConsumerRunningInfoRequestHeader header) {

        CompletableFuture<ProxyRelayResult<ConsumerRunningInfo>> responseFuture = new CompletableFuture<>();

        //1.find channel of client
        ClientChannelInfo clientChannelInfo = consumerManager.findChannel(header.getConsumerGroup(), header.getClientId());

        //2.invoke client by channel
        try {
            remotingProtocolServer.invokeToClient(clientChannelInfo.getChannel(), command, DEFAULT_MQ_CLIENT_TIMEOUT)
                    .thenAccept(response -> {
                        if (response.getCode() == ResponseCode.SUCCESS) {
                            ConsumerRunningInfo consumerRunningInfo = ConsumerRunningInfo.decode(response.getBody(), ConsumerRunningInfo.class);
                            responseFuture.complete(new ProxyRelayResult<>(response.getCode(),
                                    response.getRemark(), consumerRunningInfo));
                        } else {
                            String errMsg = String.format("get consumer running info failed, code:%s remark:%s", response.getCode(), response.getRemark());
                            RuntimeException e = new RuntimeException(errMsg);
                            responseFuture.completeExceptionally(e);
                        }
                    }).exceptionally(t -> {
                        responseFuture.completeExceptionally(ExceptionUtils.getRealException(t));
                        return null;
                    });
        } catch (Throwable t) {
            responseFuture.completeExceptionally(ExceptionUtils.getRealException(t));
            return null;
        }
        return responseFuture;
    }

    @Override
    public CompletableFuture<ProxyRelayResult<ConsumeMessageDirectlyResult>> processConsumeMessageDirectly(
        ProxyContext context, RemotingCommand command, ConsumeMessageDirectlyResultRequestHeader header) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RelayData<TransactionData, Void> processCheckTransactionState(ProxyContext context, RemotingCommand command,
        CheckTransactionStateRequestHeader header, MessageExt messageExt) {
        TransactionData transactionData = new TransactionData("", header.getTranStateTableOffset(), header.getCommitLogOffset(), header.getTransactionId(), System.currentTimeMillis(), 15_000L);
        return new RelayData<>(transactionData, new CompletableFuture<>());
    }
}
