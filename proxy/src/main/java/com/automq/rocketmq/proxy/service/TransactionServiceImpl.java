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

import java.util.List;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.service.transaction.EndTransactionRequestData;
import org.apache.rocketmq.proxy.service.transaction.TransactionData;
import org.apache.rocketmq.proxy.service.transaction.TransactionService;
import org.apache.rocketmq.remoting.protocol.header.EndTransactionRequestHeader;

public class TransactionServiceImpl implements TransactionService {
    @Override
    public void addTransactionSubscription(ProxyContext ctx, String group, List<String> topicList) {
    }

    @Override
    public void addTransactionSubscription(ProxyContext ctx, String group, String topic) {
    }

    @Override
    public void replaceTransactionSubscription(ProxyContext ctx, String group, List<String> topicList) {
    }

    @Override
    public void unSubscribeAllTransactionTopic(ProxyContext ctx, String group) {
    }

    @Override
    public TransactionData addTransactionDataByBrokerAddr(ProxyContext ctx, String brokerAddr, String producerGroup,
        long tranStateTableOffset, long commitLogOffset, String transactionId, Message message) {
        return null;
    }

    @Override
    public TransactionData addTransactionDataByBrokerName(ProxyContext ctx, String brokerName, String producerGroup,
        long tranStateTableOffset, long commitLogOffset, String transactionId, Message message) {
        return null;
    }

    @Override
    public EndTransactionRequestData genEndTransactionRequestHeader(ProxyContext ctx, String producerGroup,
        Integer commitOrRollback, boolean fromTransactionCheck, String msgId, String transactionId) {
        EndTransactionRequestHeader requestHeader = new EndTransactionRequestHeader();
        requestHeader.setProducerGroup(producerGroup);
        requestHeader.setCommitOrRollback(commitOrRollback);
        requestHeader.setFromTransactionCheck(fromTransactionCheck);
        requestHeader.setMsgId(msgId);
        requestHeader.setTransactionId(transactionId);
        return new EndTransactionRequestData("", requestHeader);
    }

    @Override
    public void onSendCheckTransactionStateFailed(ProxyContext context, String producerGroup,
        TransactionData transactionData) {
    }
}
