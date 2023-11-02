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

package com.automq.rocketmq.cli;

import apache.rocketmq.common.v1.Code;
import apache.rocketmq.controller.v1.CreateGroupReply;
import apache.rocketmq.controller.v1.CreateGroupRequest;
import apache.rocketmq.controller.v1.GroupType;
import apache.rocketmq.controller.v1.SubscriptionMode;
import com.automq.rocketmq.cli.tools.CliUtils;
import com.automq.rocketmq.common.PrefixThreadFactory;
import com.automq.rocketmq.common.exception.ControllerException;
import com.automq.rocketmq.controller.client.GrpcControllerClient;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.RateLimiter;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;
import picocli.CommandLine;

@CommandLine.Command(name = "consumeMessage", mixinStandardHelpOptions = true, showDefaultValues = true)
public class ConsumeMessage implements Callable<Void> {
    @CommandLine.Option(names = {"-t", "--topicNums"}, description = "Number of topics")
    int topicNums = 1;

    @CommandLine.Option(names = {"-r", "--rate"}, description = "Consume message rate per second across all topics")
    int messageRate = 100;

    @CommandLine.Option(names = {"-m", "--messageNums"}, description = "Number of messages")
    int numMessages = Integer.MAX_VALUE;

    @CommandLine.Option(names = {"-tp", "--topicPrefix"}, description = "The prefix of the created topics")
    String topicPrefix = "Benchmark_Topic_";

    @CommandLine.Option(names = {"-c", "--consumerNums"}, description = "Number of consumers, scale out consumer to improve throughput")
    int consumerNums = 1;

    @CommandLine.Option(names = {"-d", "--duration"}, description = "Duration in seconds")
    int durationInSeconds = 10;

    @CommandLine.Option(names = {"-i", "--reportIntervalInSeconds"}, description = "Report interval in seconds")
    int reportIntervalInSeconds = 3;

    @CommandLine.Option(names = {"-g", "--groupName"}, description = "Group name")
    String groupName = "Benchmark_Group";

    @CommandLine.Option(names = {"-gt", "--groupType"}, description = "Group type")
    GroupType groupType = GroupType.GROUP_TYPE_STANDARD;

    @CommandLine.Option(names = {"-lt", "--longPollingTimeoutInSeconds"}, description = "Long polling timeout in seconds")
    int longPollingTimeoutInSeconds = 30;

    @CommandLine.Option(names = {"-bs", "--batchSize"}, description = "Batch size per receive request")
    int batchSize = 32;

    @CommandLine.Option(names = {"-im", "--inflightMessageNums"}, description = "Inflight message number")
    int inflightMessageNums = 100;

    @CommandLine.ParentCommand
    MQAdmin mqAdmin;

    private final MetricRegistry metrics = new MetricRegistry();

    @SuppressWarnings({"ResultOfMethodCallIgnored", "UnstableApiUsage"})
    @Override
    public Void call() throws Exception {
        prepareConsumerGroup(groupName);
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        SimpleConsumer[] consumers = IntStream.range(0, consumerNums)
            .mapToObj(i -> prepareConsumer(provider, groupName)).toArray(SimpleConsumer[]::new);

        ExecutorService executor = Executors.newCachedThreadPool(new PrefixThreadFactory("Benchmark-Consumer"));
        ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
            .build();
        reporter.start(reportIntervalInSeconds, TimeUnit.SECONDS);
        Meter consumeMeter = metrics.meter("Meter for consuming messages");
        Meter ackMeter = metrics.meter("Meter for acking messages");
        long startTimestamp = System.currentTimeMillis();

        executor.submit(() -> {
            RateLimiter rateLimiter = RateLimiter.create(messageRate);
            AtomicInteger messageReceived = new AtomicInteger();
            AtomicInteger currentInflightMessages = new AtomicInteger();
            int index = 0;
            while (true) {
                if (System.currentTimeMillis() - startTimestamp > durationInSeconds * 1000L) {
                    break;
                }
                if (messageReceived.get() >= numMessages) {
                    break;
                }

                if (currentInflightMessages.get() >= inflightMessageNums) {
                    try {
                        TimeUnit.MILLISECONDS.sleep(1);
                    } catch (InterruptedException ignored) {
                    }
                    continue;
                }

                rateLimiter.acquire();

                SimpleConsumer selectedConsumer = consumers[index++ % consumerNums];
                CompletableFuture<List<MessageView>> receiveCf = selectedConsumer.receiveAsync(batchSize, Duration.ofSeconds(10));
                receiveCf.whenComplete((messageViews, throwable) -> {

                    if (throwable != null) {
                        System.out.println("Failed to receive messages: " + throwable.getMessage());
                        return;
                    }
                    if (messageViews != null) {
                        for (MessageView messageView : messageViews) {
                            consumeMeter.mark();
                            selectedConsumer.ackAsync(messageView).whenComplete((ackReceipt, ackThrowable) -> {
                                currentInflightMessages.decrementAndGet();
                                if (ackThrowable != null) {
                                    System.out.println("Failed to ack message: " + ackThrowable.getMessage());
                                } else {
                                    ackMeter.mark();
                                }
                            });
                        }
                        currentInflightMessages.addAndGet(messageViews.size());
                        messageReceived.addAndGet(messageViews.size());
                    }
                });
            }
        });

        executor.awaitTermination(durationInSeconds + 1, TimeUnit.SECONDS);
        for (SimpleConsumer consumer : consumers) {
            consumer.close();
        }
        reporter.close();
        return null;
    }

    private void prepareConsumerGroup(String consumerGroup) throws IOException {
        GrpcControllerClient client = new GrpcControllerClient(new CliClientConfig());
        CreateGroupRequest request = CreateGroupRequest.newBuilder()
            .setName(groupName)
            .setMaxDeliveryAttempt(16)
            .setGroupType(groupType)
            .setSubMode(SubscriptionMode.SUB_MODE_POP)
            .build();

        CompletableFuture<CreateGroupReply> groupCf = client.createGroup(mqAdmin.endpoint, request);
        groupCf = groupCf.exceptionally(throwable -> {
            Throwable t = CliUtils.getRealException(throwable);
            if (t instanceof ControllerException controllerException) {
                if (controllerException.getErrorCode() == Code.DUPLICATED_VALUE) {
                    System.out.printf("Group already exists: %s, just use it%n", consumerGroup);
                    return null;
                }
            }
            // Re-throw the exception to abort the program
            throw new CompletionException(t);
        });

        if (groupCf.join() != null) {
            System.out.println("Group created: " + groupName);
        }

        client.close();
    }
    private SimpleConsumer prepareConsumer(ClientServiceProvider provider, String consumerGroup) {
        StaticSessionCredentialsProvider staticSessionCredentialsProvider =
            new StaticSessionCredentialsProvider(mqAdmin.accessKey, mqAdmin.secretKey);

        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
            .setEndpoints(mqAdmin.endpoint)
            .setCredentialProvider(staticSessionCredentialsProvider)
            .setRequestTimeout(Duration.ofSeconds(10))
            .build();

        FilterExpression filterExpression = new FilterExpression("*", FilterExpressionType.TAG);
        Map<String, FilterExpression> subs = new ConcurrentHashMap<>();
        IntStream.range(0, topicNums).mapToObj(i -> topicPrefix + i).forEach(topic -> subs.put(topic, filterExpression));

        try {
            return provider.newSimpleConsumerBuilder()
                .setClientConfiguration(clientConfiguration)
                .setConsumerGroup(consumerGroup)
                .setSubscriptionExpressions(subs)
                .setAwaitDuration(Duration.ofSeconds(longPollingTimeoutInSeconds))
                .build();
        } catch (ClientException e) {
            throw new CompletionException(e);
        }
    }
}
