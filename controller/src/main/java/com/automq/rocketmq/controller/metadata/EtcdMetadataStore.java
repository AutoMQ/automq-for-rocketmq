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

import apache.rocketmq.controller.v1.Code;
import com.automq.rocketmq.common.PrefixThreadFactory;
import com.automq.rocketmq.controller.exception.ControllerException;
import com.google.common.base.Preconditions;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Cluster;
import io.etcd.jetcd.Election;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.Txn;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.election.CampaignResponse;
import io.etcd.jetcd.election.LeaderKey;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EtcdMetadataStore implements MetadataStore, AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(EtcdMetadataStore.class);

    private static final int LEASE_TTL_IN_SECS = 3;
    private static final int KEEP_ALIVE_LEASE_INTERVAL_IN_SECS = 2;

    private final String clusterName;

    /**
     * Nested etcd client.
     */
    private final Client etcdClient;

    private final Lease lease;

    private final KV kv;

    private final Watch watch;

    private final Election election;

    private final Cluster cluster;

    private final String leaderElectionName;

    private ScheduledExecutorService executorService;

    private long leaseId;

    private Role role;

    private LeaderKey leaderKey;

    private UUID term;

    public EtcdMetadataStore(Iterable<URI> endpoints, String clusterName) throws ControllerException {
        this.clusterName = clusterName;
        this.leaderElectionName = String.format("/%s/leader", clusterName);
        this.etcdClient = Client.builder().endpoints(endpoints).build();
        this.lease = this.etcdClient.getLeaseClient();
        this.kv = this.etcdClient.getKVClient();
        this.watch = this.etcdClient.getWatchClient();
        this.election = this.etcdClient.getElectionClient();
        this.cluster = this.etcdClient.getClusterClient();
        this.role = Role.Follower;
        this.executorService = new ScheduledThreadPoolExecutor(2, new PrefixThreadFactory("EtcdMetadataStore"),
            new ThreadPoolExecutor.AbortPolicy());
        this.runLease();
        this.runWatch();
        this.runElection();
    }

    private void runLease() throws ControllerException {
        try {
            LeaseGrantResponse response = this.lease.grant(LEASE_TTL_IN_SECS).get();
            this.leaseId = response.getID();
            // Renew lease periodically
            executorService.scheduleAtFixedRate(() -> this.lease.keepAliveOnce(this.leaseId),
                KEEP_ALIVE_LEASE_INTERVAL_IN_SECS,
                KEEP_ALIVE_LEASE_INTERVAL_IN_SECS,
                TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException e) {
            throw new ControllerException(Code.INTERRUPTED_VALUE, e);
        }
    }

    private void runElection() {
        // Ensure runLease has run.
        Preconditions.checkState(this.leaseId > 0, "Should have gotten an lease");
        ByteSequence electionName = ByteSequence.from(this.leaderElectionName, StandardCharsets.UTF_8);
        ByteSequence proposal = ByteSequence.from("dummy", StandardCharsets.UTF_8);
        this.election.campaign(electionName, this.leaseId, proposal)
            .thenAccept(this::onElected);
    }

    /**
     * Handle election campaign winning.
     *
     * Procedure is:
     * <ol>
     *     <li>Change controller role to Elected</li>
     *     <li>Modify term</li>
     * </ol>
     * @param campaignResponse The campaign response.
     */
    private void onElected(CampaignResponse campaignResponse) {
        this.leaderKey = campaignResponse.getLeader();
        LOGGER.info("Campaign completes and gets elected as leader of {}", this.leaderKey.getName());
        this.role = Role.Elected;

        // Now that this controller node is elected as leader, we update controller term and wait till CDC catches up.
        // Once the watching receives the latest term change, this controller node is safe to act as leader, because
        // all previous changes are definitely applied.
        this.term = UUID.randomUUID();
        ByteSequence termKey = ByteSequence.from(String.format("/%s/term", this.clusterName), StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putLong(this.term.getMostSignificantBits());
        buffer.putLong(this.term.getLeastSignificantBits());
        buffer.flip();
        ByteSequence termValue = ByteSequence.from(buffer.array());
        Txn tx = this.kv.txn();

        tx.If(new Cmp(this.leaderKey.getKey(),
                Cmp.Op.EQUAL,
                CmpTarget.modRevision(this.leaderKey.getRevision())))
            .Then(Op.put(termKey, termValue, PutOption.builder().withLeaseId(this.leaseId).build()))
            .commit().thenAccept(response -> {
                if (response.isSucceeded()) {
                    LOGGER.info("Term of {} updated", this.clusterName);
                }
            });
    }

    private void runWatch() {
        ByteSequence watchPrefix = ByteSequence.from(String.format("/%s", this.clusterName), StandardCharsets.UTF_8);
        this.watch.watch(watchPrefix, WatchOption.builder().isPrefix(true).build(), new WatchListener(this));
    }

    /**
     * Part of watch listener.
     *
     * @param response Watch response.
     */
    void onWatch(WatchResponse response) {
        LOGGER.debug("Got a watch response");
        for (WatchEvent event : response.getEvents()) {
            processWatchEvent(event);
        }
    }

    /**
     * Process WatchEvent from etcd cluster.
     *
     * Actions to each watch event vary according to the associated key path.
     *
     * @param event Watch event to handle
     */
    private void processWatchEvent(WatchEvent event) {
        ByteSequence termKey = ByteSequence.from(String.format("/%s/term", this.clusterName), StandardCharsets.UTF_8);
        KeyValue kv = event.getKeyValue();
        if (kv.getKey().equals(termKey)) {
            ByteBuffer buf = ByteBuffer.wrap(kv.getValue().getBytes());
            if (buf.limit() >= 16) {
                UUID term = new UUID(buf.getLong(), buf.getLong());
                if (this.term.equals(term) && this.role == Role.Elected) {
                    this.role = Role.Leader;
                    LOGGER.info("Now acts as Controller leader");
                }
            }

        }

    }

    void onWatchError(Throwable cause) {
        LOGGER.error("Watch failed", cause);
    }

    void onWatchComplete() {
        LOGGER.info("Watch completed");
    }

    @Override
    public long registerBroker(int brokerId) throws IOException {

        return 0;
    }

    @Override
    public void close() throws ControllerException {
        this.executorService.shutdown();
    }

    public Role getRole() {
        return role;
    }
}
