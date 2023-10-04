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

import java.util.List;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.service.transaction.EndTransactionRequestData;
import org.apache.rocketmq.proxy.service.transaction.TransactionData;
import org.apache.rocketmq.proxy.service.transaction.TransactionService;

public class TransactionServiceImpl implements TransactionService {
    @Override
    public void addTransactionSubscription(ProxyContext ctx, String group, List<String> topicList) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addTransactionSubscription(ProxyContext ctx, String group, String topic) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void replaceTransactionSubscription(ProxyContext ctx, String group, List<String> topicList) {
        throw new UnsupportedOperationException();

    }

    @Override
    public void unSubscribeAllTransactionTopic(ProxyContext ctx, String group) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TransactionData addTransactionDataByBrokerAddr(ProxyContext ctx, String brokerAddr, String producerGroup,
        long tranStateTableOffset, long commitLogOffset, String transactionId, Message message) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TransactionData addTransactionDataByBrokerName(ProxyContext ctx, String brokerName, String producerGroup,
        long tranStateTableOffset, long commitLogOffset, String transactionId, Message message) {
        throw new UnsupportedOperationException();
    }

    @Override
    public EndTransactionRequestData genEndTransactionRequestHeader(ProxyContext ctx, String producerGroup,
        Integer commitOrRollback, boolean fromTransactionCheck, String msgId, String transactionId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onSendCheckTransactionStateFailed(ProxyContext context, String producerGroup,
        TransactionData transactionData) {
        throw new UnsupportedOperationException();
    }
}
