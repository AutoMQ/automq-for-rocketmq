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

package com.automq.rocketmq.controller.metadata;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Election;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.Txn;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.election.CampaignResponse;
import io.etcd.jetcd.election.LeaderKey;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.TxnResponse;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.testcontainers.shaded.org.awaitility.Awaitility;

/**
 * Test basic usage of Etcd Java binding.
 */
public class EtcdTest extends EtcdTestBase {

    @Test
    public void testKV() throws Exception {
        try (Client client = Client.builder().endpoints(CLUSTER_EXTENSION.clientEndpoints()).build();
             KV kv = client.getKVClient()) {
            kv.put(ByteSequence.from("key".getBytes()), ByteSequence.from("value".getBytes())).get();
            GetResponse response = kv.get(ByteSequence.from("key".getBytes())).get();
            Assertions.assertEquals(1, response.getCount());
        }

        try (Client client = Client.builder().endpoints(CLUSTER_EXTENSION.clientEndpoints()).build();
             KV kv = client.getKVClient();
             Lease lease = client.getLeaseClient()) {
            LeaseGrantResponse leaseGrantResponse = lease.grant(1).get();
            long leaseId = leaseGrantResponse.getID();

            kv.put(ByteSequence.from("key1".getBytes()),
                ByteSequence.from("value1".getBytes()),
                PutOption.builder().withLeaseId(leaseId).build()).get();

            // We may optionally revoke the lease to make the transient node disappear faster.
            // lease.revoke(leaseId);

            Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .with()
                .pollInterval(Duration.ofMillis(100))
                .until(() -> {
                    GetResponse response = kv.get(ByteSequence.from("key1".getBytes())).get();
                    return response.getCount() == 0;
                });
        }
    }

    @Test
    public void testEtcdElection() throws InterruptedException {
        ByteSequence electionName = ByteSequence.from("leader-election", Charset.defaultCharset());

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicInteger term = new AtomicInteger(0);

        Runnable task = () -> {
            try (Client client = Client.builder().endpoints(CLUSTER_EXTENSION.clientEndpoints()).build();
                 Lease lease = client.getLeaseClient();
                 Election election = client.getElectionClient()) {
                LeaseGrantResponse leaseGrantResponse = lease.grant(3).get();
                long leaseId = leaseGrantResponse.getID();
                lease.keepAliveOnce(leaseId);
                String threadName = Thread.currentThread().getName();
                ByteSequence proposal = ByteSequence.from(threadName, Charset.defaultCharset());
                election.campaign(electionName, leaseId, proposal).thenAccept(response -> {
                    LeaderKey leaderKey = response.getLeader();
                    term.incrementAndGet();
                    latch.countDown();
                    LOGGER.info("{} now is leader with leader-key: {}", threadName, leaderKey.getName());
                });
                latch.await();
            } catch (ExecutionException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        };

        for (int i = 0; i < 2; i++) {
            Thread t = new Thread(task);
            t.start();
        }
        latch.await();

        Assertions.assertEquals(term.get(), 1);
    }

    @Test
    public void testEtcdResign() throws InterruptedException {
        ByteSequence electionName = ByteSequence.from("leader-resign", Charset.defaultCharset());

        final CountDownLatch latch = new CountDownLatch(2);
        final AtomicInteger term = new AtomicInteger(0);

        Runnable task = () -> {
            try (Client client = Client.builder().endpoints(CLUSTER_EXTENSION.clientEndpoints()).build();
                 Lease lease = client.getLeaseClient();
                 Election election = client.getElectionClient()) {
                LeaseGrantResponse leaseGrantResponse = lease.grant(3).get();
                long leaseId = leaseGrantResponse.getID();
                lease.keepAliveOnce(leaseId);
                String threadName = Thread.currentThread().getName();
                ByteSequence proposal = ByteSequence.from(threadName, Charset.defaultCharset());
                election.campaign(electionName, leaseId, proposal).thenApply(response -> {
                    LeaderKey leaderKey = response.getLeader();
                    term.incrementAndGet();
                    latch.countDown();
                    LOGGER.info("{} now is leader with leader-key: {}", threadName, leaderKey.getName());
                    return leaderKey;
                }).thenAccept(election::resign);
                latch.await();
            } catch (ExecutionException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        };

        for (int i = 0; i < 2; i++) {
            Thread t = new Thread(task);
            t.start();
        }
        latch.await();

        Assertions.assertEquals(term.get(), 2);

    }

    @Test
    public void testEtcdTransaction() throws Exception {
        try (Client client = Client.builder().endpoints(CLUSTER_EXTENSION.clientEndpoints()).build();
             Lease lease = client.getLeaseClient();
             Election election = client.getElectionClient();
             KV kv = client.getKVClient();
             Watch watch = client.getWatchClient()) {
            LeaseGrantResponse grantResponse = lease.grant(3).get();
            long leaseId = grantResponse.getID();
            lease.keepAliveOnce(leaseId);
            ByteSequence electionName = ByteSequence.from("leader-txn".getBytes());
            ByteSequence proposal = ByteSequence.from("127.0.0.1".getBytes());
            CampaignResponse campaignResponse = election.campaign(electionName, leaseId, proposal).get();
            LeaderKey leaderKey = campaignResponse.getLeader();

            watch.watch(ByteSequence.from("/brokers", Charset.defaultCharset()),
                WatchOption.builder().isPrefix(true).build(),
                new Watch.Listener() {
                    @Override
                    public void onNext(WatchResponse response) {
                        List<WatchEvent> events = response.getEvents();
                        for (WatchEvent event : events) {
                            WatchEvent.EventType type = event.getEventType();
                            KeyValue kv = event.getKeyValue();
                            switch (type) {
                                case PUT -> {
                                    LOGGER.info("Put {} --> {}", kv.getKey(), kv.getValue());
                                }
                                case DELETE -> {
                                    LOGGER.info("Delete {} --> {}", kv.getKey(), kv.getValue());
                                }
                            }
                        }
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        LOGGER.error("Watch brokers failed", throwable);
                    }

                    @Override
                    public void onCompleted() {
                        LOGGER.info("Watch completed");
                    }
                });

            kv.put(ByteSequence.from("/brokers/broker-a", Charset.defaultCharset()),
                ByteSequence.from("broker-a", Charset.defaultCharset()),
                PutOption.builder().withLeaseId(leaseId).build());

            // Transient node
            // kv.put(null, null, PutOption.builder().withLeaseId(leaseId).build());
            Txn txn = kv.txn();
            TxnResponse txnResponse = txn.If(new Cmp(leaderKey.getKey(), Cmp.Op.EQUAL,
                    CmpTarget.value(proposal)))
                .Then(Op.put(ByteSequence.from("abc".getBytes()), ByteSequence.from("def".getBytes()), PutOption.DEFAULT))
                .commit().get();
            Assertions.assertTrue(txnResponse.isSucceeded());
        }
    }
}