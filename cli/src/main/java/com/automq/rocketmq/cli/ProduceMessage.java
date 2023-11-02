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

import apache.rocketmq.controller.v1.AcceptTypes;
import apache.rocketmq.common.v1.Code;
import apache.rocketmq.controller.v1.CreateTopicRequest;
import apache.rocketmq.controller.v1.MessageType;
import com.automq.rocketmq.cli.tools.CliUtils;
import com.automq.rocketmq.common.PrefixThreadFactory;
import com.automq.rocketmq.common.exception.ControllerException;
import com.automq.rocketmq.controller.client.GrpcControllerClient;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.RateLimiter;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.producer.Producer;
import picocli.CommandLine;

@CommandLine.Command(name = "produceMessage", mixinStandardHelpOptions = true, showDefaultValues = true)
public class ProduceMessage implements Callable<Void> {
    @CommandLine.Option(names = {"-t", "--topicNums"}, description = "Number of topics")
    int topicNums = 1;

    @CommandLine.Option(names = {"-q", "--queueNums"}, description = "Number of queues per topic")
    int queueNums = 1;

    @CommandLine.Option(names = {"-p", "--producerNums"}, description = "Number of producers, scale out producer to improve throughput")
    int producerNums = 1;

    @CommandLine.Option(names = {"-s", "--size"}, description = "Message payload size in byte")
    int messageSize = 1024;

    @CommandLine.Option(names = {"-r", "--rate"}, description = "Publish message rate per second across all topics")
    int messageRate = 100;

    @CommandLine.Option(names = {"-m", "--messageNums"}, description = "Number of messages")
    int numMessages = Integer.MAX_VALUE;

    @CommandLine.Option(names = {"-tp", "--topicPrefix"}, description = "The prefix of the created topics")
    String topicPrefix = "Benchmark_Topic_";

    @CommandLine.Option(names = {"-mt", "--messageType"}, description = "Message type")
    MessageType messageType = MessageType.NORMAL;

    @CommandLine.Option(names = {"-d", "--duration"}, description = "Duration in seconds")
    int durationInSeconds = 10;

    @CommandLine.Option(names = {"-i", "--reportIntervalInSeconds"}, description = "Report interval in seconds")
    int reportIntervalInSeconds = 3;

    @CommandLine.ParentCommand
    MQAdmin mqAdmin;

    private final MetricRegistry metrics = new MetricRegistry();

    @SuppressWarnings({"ResultOfMethodCallIgnored", "UnstableApiUsage"})
    @Override
    public Void call() throws Exception {
        prepareTopics();
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        Producer[] producers = IntStream.range(0, producerNums).mapToObj(i -> prepareProducer(provider)).toArray(Producer[]::new);

        ExecutorService executor = Executors.newCachedThreadPool(new PrefixThreadFactory("Benchmark-Producer"));
        ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
            .build();
        reporter.start(reportIntervalInSeconds, TimeUnit.SECONDS);
        Timer timer = metrics.timer("Timer for sending messages");
        long startTimestamp = System.currentTimeMillis();

        executor.submit(() -> {
            RateLimiter rateLimiter = RateLimiter.create(messageRate);
            byte[] payload = randomPayload();

            for (int i = 0; i < numMessages; i++) {
                if (System.currentTimeMillis() - startTimestamp > durationInSeconds * 1000L) {
                    break;
                }
                try {
                    String topic = topicPrefix + (i % topicNums);
                    Message message = provider.newMessageBuilder()
                        .setTopic(topic)
                        .setBody(payload)
                        .build();

                    rateLimiter.acquire();
                    long start = System.currentTimeMillis();
                    producers[i % producerNums].sendAsync(message).thenAccept(sendReceipt -> {
                        timer.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
                    });
                } catch (Exception e) {
                    System.out.println("Failed to send message: " + e.getMessage());
                }
            }
        });
        executor.awaitTermination(durationInSeconds + 1, TimeUnit.SECONDS);
        for (Producer producer : producers) {
            producer.close();
        }
        reporter.close();
        return null;
    }

    private void prepareTopics() throws IOException, ControllerException {
        GrpcControllerClient client = new GrpcControllerClient(new CliClientConfig());

        for (int i = 0; i < topicNums; i++) {
            String topicName = topicPrefix + i;
            CreateTopicRequest request = CreateTopicRequest.newBuilder()
                .setTopic(topicName)
                .setCount(queueNums)
                .setAcceptTypes(AcceptTypes.newBuilder().addTypes(messageType).build())
                .build();

            CompletableFuture<Long> topicCf = client.createTopic(mqAdmin.endpoint, request);

            topicCf = topicCf.exceptionally(throwable -> {
                Throwable t = CliUtils.getRealException(throwable);
                if (t instanceof ControllerException controllerException) {
                    if (controllerException.getErrorCode() == Code.DUPLICATED_VALUE) {
                        System.out.printf("Topic already exists: %s, just use it%n", topicName);
                        return -1L;
                    }
                }
                // Re-throw the exception to abort the program
                throw new CompletionException(t);
            });
            topicCf.join();
        }
        System.out.printf("Topics created from %s to %s%n", topicPrefix + 0, topicPrefix + (topicNums - 1));
        client.close();
    }

    private Producer prepareProducer(ClientServiceProvider provider) {
        StaticSessionCredentialsProvider staticSessionCredentialsProvider =
            new StaticSessionCredentialsProvider(mqAdmin.accessKey, mqAdmin.secretKey);

        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
            .setEndpoints(mqAdmin.endpoint)
            .setCredentialProvider(staticSessionCredentialsProvider)
            .setRequestTimeout(Duration.ofSeconds(10))
            .build();

        try {
            return provider.newProducerBuilder()
                .setTopics(IntStream.range(0, topicNums).mapToObj(i -> topicPrefix + i).toArray(String[]::new))
                .setClientConfiguration(clientConfiguration)
                .build();
        } catch (ClientException e) {
            throw new CompletionException(e);
        }
    }

    private byte[] randomPayload() {
        byte[] payload = new byte[messageSize];
        for (int i = 0; i < messageSize; i++) {
            payload[i] = (byte) (Math.random() * 256);
        }
        return payload;
    }
}
