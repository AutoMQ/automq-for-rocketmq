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

import io.etcd.jetcd.launcher.EtcdCluster;
import io.etcd.jetcd.test.EtcdClusterExtension;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EtcdTestBase {
    public static final Logger LOGGER = LoggerFactory.getLogger(EtcdTest.class);

    @RegisterExtension
    public static final EtcdClusterExtension CLUSTER_EXTENSION = EtcdClusterExtension.builder().withNodes(1).build();

    public static EtcdCluster cluster = CLUSTER_EXTENSION.cluster();

    @BeforeClass
    public static void setUp() {
        cluster.start();
    }

    @AfterClass
    public static void tearDown() {
        cluster.close();
    }
}
