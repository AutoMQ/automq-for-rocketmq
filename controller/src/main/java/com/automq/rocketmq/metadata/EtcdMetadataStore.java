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

import io.etcd.jetcd.Client;
import java.io.IOException;

public class EtcdMetadataStore implements MetadataStore {

    /**
     * Etcd target.
     *
     * dns:///foo.bar.com:2379
     * ip:///etcd0:2379,etcd1:2379,etcd2:2379
     */
    private final String target;

    /**
     * Nested etcd client.
     */
    private final Client etcdClient;

    public EtcdMetadataStore(String target) {
        this.target = target;
        this.etcdClient = Client.builder().target(target).build();
    }

    @Override
    public long registerBroker(int brokerId) throws IOException {

        return 0;
    }
}
