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

package com.automq.rocketmq.metadata;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.launcher.EtcdCluster;
import io.etcd.jetcd.test.EtcdClusterExtension;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.RegisterExtension;

public class EtcdMetadataStoreTest {

    @RegisterExtension
    public static final EtcdClusterExtension clusterExtension = EtcdClusterExtension.builder().withNodes(1).build();

    @Test
    public void testEtcdServer() throws Exception {
        try (final EtcdCluster cluster = clusterExtension.cluster()) {
            cluster.start();
            try (
                Client client = Client.builder().endpoints(clusterExtension.clientEndpoints()).build()) {
                client.getKVClient().put(ByteSequence.from("key".getBytes()), ByteSequence.from("value".getBytes())).get();
                GetResponse response = client.getKVClient().get(ByteSequence.from("key".getBytes())).get();
                Assertions.assertEquals(1, response.getCount());
            }
        }
    }

}