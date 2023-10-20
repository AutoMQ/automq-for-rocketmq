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
import apache.rocketmq.v2.FilterExpression;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.Subscription;
import com.automq.rocketmq.proxy.grpc.v2.consumer.ExtendReceiveMessageResponseStreamWriter;
import com.automq.rocketmq.proxy.model.VirtualQueue;
import com.automq.rocketmq.proxy.processor.ExtendMessagingProcessor;
import com.automq.rocketmq.proxy.service.SuspendPopRequestService;
import com.google.protobuf.util.Durations;
import io.grpc.stub.StreamObserver;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.common.constant.ConsumeInitMode;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcConverter;
import org.apache.rocketmq.proxy.grpc.v2.consumer.PopMessageResultFilterImpl;
import org.apache.rocketmq.proxy.grpc.v2.consumer.ReceiveMessageActivity;
import org.apache.rocketmq.proxy.grpc.v2.consumer.ReceiveMessageResponseStreamWriter;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.processor.QueueSelector;
import org.apache.rocketmq.proxy.processor.ReceiptHandleProcessor;
import org.apache.rocketmq.proxy.service.ServiceManager;
import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;
import org.apache.rocketmq.proxy.service.route.MessageQueueView;
import org.apache.rocketmq.remoting.protocol.filter.FilterAPI;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

public class ExtendReceiveMessageActivity extends ReceiveMessageActivity {
    private static final String ILLEGAL_POLLING_TIME_INTRODUCED_CLIENT_VERSION = "5.0.3";

    private final SuspendPopRequestService suspendPopRequestService;

    public ExtendReceiveMessageActivity(MessagingProcessor messagingProcessor,
        ReceiptHandleProcessor receiptHandleProcessor,
        GrpcClientSettingsManager grpcClientSettingsManager,
        GrpcChannelManager grpcChannelManager) {
        super(messagingProcessor, receiptHandleProcessor, grpcClientSettingsManager, grpcChannelManager);
        this.suspendPopRequestService = SuspendPopRequestService.getInstance();
    }

    @Override
    protected ReceiveMessageResponseStreamWriter createWriter(ProxyContext ctx,
        StreamObserver<ReceiveMessageResponse> responseObserver) {
        return new ExtendReceiveMessageResponseStreamWriter(this.messagingProcessor, responseObserver);
    }

    @Override
    public void receiveMessage(ProxyContext ctx, ReceiveMessageRequest request,
        StreamObserver<ReceiveMessageResponse> responseObserver) {
        ReceiveMessageResponseStreamWriter writer = createWriter(ctx, responseObserver);

        try {
            Settings settings = this.grpcClientSettingsManager.getClientSettings(ctx);
            Subscription subscription = settings.getSubscription();
            boolean fifo = subscription.getFifo();
            int maxAttempts = settings.getBackoffPolicy().getMaxAttempts();
            ProxyConfig config = ConfigurationManager.getProxyConfig();

            Long timeRemaining = ctx.getRemainingMs();
            long pollingTime;
            if (request.hasLongPollingTimeout()) {
                pollingTime = Durations.toMillis(request.getLongPollingTimeout());
            } else {
                pollingTime = timeRemaining - Durations.toMillis(settings.getRequestTimeout()) / 2;
            }
            if (pollingTime < config.getGrpcClientConsumerMinLongPollingTimeoutMillis()) {
                pollingTime = config.getGrpcClientConsumerMinLongPollingTimeoutMillis();
            }
            if (pollingTime > config.getGrpcClientConsumerMaxLongPollingTimeoutMillis()) {
                pollingTime = config.getGrpcClientConsumerMaxLongPollingTimeoutMillis();
            }

            if (pollingTime > timeRemaining) {
                if (timeRemaining >= config.getGrpcClientConsumerMinLongPollingTimeoutMillis()) {
                    pollingTime = timeRemaining;
                } else {
                    final String clientVersion = ctx.getClientVersion();
                    Code code =
                        null == clientVersion || ILLEGAL_POLLING_TIME_INTRODUCED_CLIENT_VERSION.compareTo(clientVersion) > 0 ?
                            Code.BAD_REQUEST : Code.ILLEGAL_POLLING_TIME;
                    writer.writeAndComplete(ctx, code, "The deadline time remaining is not enough" +
                        " for polling, please check network condition");
                    return;
                }
            }

            validateTopicAndConsumerGroup(request.getMessageQueue().getTopic(), request.getGroup());
            String topic = GrpcConverter.getInstance().wrapResourceWithNamespace(request.getMessageQueue().getTopic());
            String group = GrpcConverter.getInstance().wrapResourceWithNamespace(request.getGroup());

            long actualInvisibleTime = Durations.toMillis(request.getInvisibleDuration());
            ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
            if (proxyConfig.isEnableProxyAutoRenew() && request.getAutoRenew()) {
                actualInvisibleTime = proxyConfig.getDefaultInvisibleTimeMills();
            } else {
                validateInvisibleTime(actualInvisibleTime,
                    ConfigurationManager.getProxyConfig().getMinInvisibleTimeMillsForRecv());
            }

            FilterExpression filterExpression = request.getFilterExpression();
            SubscriptionData subscriptionData;
            try {
                subscriptionData = FilterAPI.build(topic, filterExpression.getExpression(),
                    GrpcConverter.getInstance().buildExpressionType(filterExpression.getType()));
            } catch (Exception e) {
                writer.writeAndComplete(ctx, Code.ILLEGAL_FILTER_EXPRESSION, e.getMessage());
                return;
            }

            receiveMessage(ctx, request, topic, group, subscriptionData, fifo, actualInvisibleTime, pollingTime,
                maxAttempts, timeRemaining, writer);

        } catch (Throwable t) {
            writer.writeAndComplete(ctx, request, t);
        }
    }

    public static class FakeQueueSelector implements QueueSelector {
        private final AddressableMessageQueue messageQueue;

        public FakeQueueSelector(AddressableMessageQueue messageQueue) {
            this.messageQueue = messageQueue;
        }

        @Override
        public AddressableMessageQueue select(ProxyContext ctx, MessageQueueView messageQueueView) {
            return messageQueue;
        }
    }

    public void receiveMessage(ProxyContext ctx, ReceiveMessageRequest request, String topic,
        String group, SubscriptionData subscriptionData, boolean fifo, long actualInvisibleTime, long pollingTime,
        int maxAttempts, long timeRemaining, ReceiveMessageResponseStreamWriter writer) {
        try {
            QueueSelector actualSelector;
            int queueId;
            {
                ServiceManager serviceManager = ((ExtendMessagingProcessor) this.messagingProcessor).getServiceManager();
                ReceiveMessageQueueSelector selector = new ReceiveMessageQueueSelector(
                    request.getMessageQueue().getBroker().getName()
                );
                AddressableMessageQueue messageQueue = selector.select(ctx, serviceManager.getTopicRouteService().getCurrentMessageQueueView(ctx, topic));
                actualSelector = new FakeQueueSelector(messageQueue);
                queueId = new VirtualQueue(messageQueue).physicalQueueId();
            }

            this.messagingProcessor.popMessage(
                    ctx,
                    actualSelector,
                    group,
                    topic,
                    request.getBatchSize(),
                    actualInvisibleTime,
                    pollingTime,
                    ConsumeInitMode.MAX,
                    subscriptionData,
                    fifo,
                    new PopMessageResultFilterImpl(maxAttempts),
                    timeRemaining
                ).thenAccept(popResult -> {
                    if (popResult.getPopStatus() == PopStatus.NO_NEW_MSG) {
                        suspendPopRequestService.suspendPopRequest(ctx, request, topic, queueId, subscriptionData, timeRemaining,
                            timeoutMillis -> messagingProcessor.popMessage(
                                ctx,
                                actualSelector,
                                group,
                                topic,
                                request.getBatchSize(),
                                actualInvisibleTime,
                                pollingTime,
                                ConsumeInitMode.MAX,
                                subscriptionData,
                                fifo,
                                new PopMessageResultFilterImpl(maxAttempts),
                                timeoutMillis
                            ), writer);
                        return;
                    }
                    writer.writeAndComplete(ctx, request, popResult);
                })
                .exceptionally(t -> {
                    writer.writeAndComplete(ctx, request, t);
                    return null;
                });
        } catch (Throwable t) {
            writer.writeAndComplete(ctx, request, t);
        }
    }
}
