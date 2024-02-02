/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
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