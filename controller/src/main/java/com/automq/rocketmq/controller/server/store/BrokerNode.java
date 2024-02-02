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
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Node with runtime information.
 */
public class BrokerNode {

    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerNode.class);

    private final Node node;

    private long lastKeepAlive;

    private boolean goingAway;

    private final long baseNano;
    private final Date baseTime;

    public BrokerNode(Node node) {
        this.node = node;
        this.lastKeepAlive = System.nanoTime();
        this.baseNano = this.lastKeepAlive;
        this.baseTime = new Date();
    }

    public Date lastKeepAliveTime(ControllerConfig config) {
        if (node.getId() == config.nodeId()) {
            return new Date();
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(baseTime);
        long deltaMillis = TimeUnit.NANOSECONDS.toMillis(lastKeepAlive - baseNano);
        calendar.add(Calendar.SECOND, (int) TimeUnit.MILLISECONDS.toSeconds(deltaMillis));
        calendar.add(Calendar.MILLISECOND, (int) (deltaMillis % 1000));
        return calendar.getTime();
    }

    public void keepAlive(long epoch, boolean goingAway) {
        if (epoch < node.getEpoch()) {
            LOGGER.warn("Heartbeat epoch={} is deprecated, known epoch={}", epoch, node.getEpoch());
            return;
        }

        this.lastKeepAlive = System.nanoTime();
        this.goingAway = goingAway;
    }

    public boolean isAlive(ControllerConfig config) {
        if (node.getId() == config.nodeId()) {
            return true;
        }

        long nanos = System.nanoTime() - this.lastKeepAlive;
        return TimeUnit.NANOSECONDS.toMillis(nanos) <= TimeUnit.SECONDS.toMillis(config.nodeAliveIntervalInSecs());
    }

    public Node getNode() {
        return node;
    }

    public boolean isGoingAway() {
        return goingAway;
    }

    public void setGoingAway(boolean goingAway) {
        this.goingAway = goingAway;
    }
}
