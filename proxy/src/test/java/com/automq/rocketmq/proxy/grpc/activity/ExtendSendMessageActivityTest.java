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

package com.automq.rocketmq.proxy.grpc.activity;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.MessageType;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.SendMessageResponse;
import apache.rocketmq.v2.SendResultEntry;
import apache.rocketmq.v2.SystemProperties;
import com.automq.rocketmq.proxy.model.ProxyContextExt;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.proxy.config.Configuration;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;

class ExtendSendMessageActivityTest {
    private ExtendSendMessageActivity sendMessageActivity;

    @BeforeEach
    void setUp() throws NoSuchFieldException, IllegalAccessException {
        Field field = ConfigurationManager.class.getDeclaredField("configuration");
        field.setAccessible(true);
        Configuration configuration = new Configuration();
        configuration.setProxyConfig(new org.apache.rocketmq.proxy.config.ProxyConfig());
        field.set(null, configuration);

        MessagingProcessor messagingProcessor = Mockito.mock(MessagingProcessor.class);
        SendResult sendResult = new SendResult();
        sendResult.setSendStatus(SendStatus.SEND_OK);
        sendResult.setMsgId("123");
        sendResult.setQueueOffset(123);
        CompletableFuture<List<Object>> future = CompletableFuture.completedFuture(List.of(sendResult));
        Mockito.doReturn(future).when(messagingProcessor).sendMessage(any(), any(), any(), anyInt(), any());

        sendMessageActivity = new ExtendSendMessageActivity(
            messagingProcessor,
            Mockito.mock(GrpcClientSettingsManager.class),
            Mockito.mock(GrpcChannelManager.class)
        );
    }

    @Test
    void sendMessage() {
        SystemProperties systemProperties = SystemProperties.newBuilder()
            .setMessageId("123")
            .setMessageType(MessageType.NORMAL)
            .build();

        Message message = Message.newBuilder()
            .setTopic(Resource.newBuilder().setName("topic").build())
            .setSystemProperties(systemProperties)
            .build();

        SendMessageRequest request = SendMessageRequest.newBuilder()
            .addMessages(message)
            .build();

        CompletableFuture<SendMessageResponse> future = sendMessageActivity.sendMessage(ProxyContextExt.create(), request);
        assertNotNull(future);
        assertTrue(future.isDone());
        assertFalse(future.isCompletedExceptionally());

        SendMessageResponse response = future.join();
        assertEquals(Code.OK, response.getStatus().getCode());
        assertEquals(1, response.getEntriesList().size());

        SendResultEntry entry = response.getEntriesList().get(0);
        assertEquals("123", entry.getMessageId());
        assertEquals(Code.OK, entry.getStatus().getCode());
    }
}