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

package com.automq.rocketmq.controller.server.store;

import com.automq.rocketmq.common.config.ControllerConfig;
import com.automq.rocketmq.metadata.dao.Node;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;


class BrokerNodeTest {

    @Test
    public void testLastKeepAliveTime() throws InterruptedException {
        Node node = new Node();
        node.setId(1);
        node.setName("N1");
        BrokerNode brokerNode = new BrokerNode(node);
        ControllerConfig config = Mockito.mock(ControllerConfig.class);
        Mockito.when(config.nodeId()).thenReturn(1);
        Date date = new Date();
        Date time = brokerNode.lastKeepAliveTime(config);
        Assertions.assertTrue(date.getTime() <= time.getTime(), "Current node should be always alive");
        Assertions.assertTrue(brokerNode.isAlive(config));

        BrokerNode brokerNode1 = new BrokerNode(node);
        Mockito.when(config.nodeId()).thenReturn(2);
        TimeUnit.MILLISECONDS.sleep(100);
        Date last = brokerNode1.lastKeepAliveTime(config);
        Assertions.assertTrue(System.currentTimeMillis() - last.getTime() >= 100);

        brokerNode1.keepAlive(-1, false);
        brokerNode1.keepAlive(1, true);
    }

}