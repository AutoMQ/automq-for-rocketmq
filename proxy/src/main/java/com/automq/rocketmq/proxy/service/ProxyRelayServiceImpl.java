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

import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayResult;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.apache.rocketmq.proxy.service.relay.RelayData;
import org.apache.rocketmq.proxy.service.transaction.TransactionData;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerRunningInfoRequestHeader;

public class ProxyRelayServiceImpl implements ProxyRelayService {
    @Override
    public CompletableFuture<ProxyRelayResult<ConsumerRunningInfo>> processGetConsumerRunningInfo(ProxyContext context,
        RemotingCommand command, GetConsumerRunningInfoRequestHeader header) {
        throw new UnsupportedOperationException();
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
