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

package com.automq.rocketmq.controller.server.tasks;

import com.automq.rocketmq.common.exception.ControllerException;
import com.automq.rocketmq.controller.MetadataStore;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ControllerTask implements Runnable {
    protected static final Logger LOGGER = LoggerFactory.getLogger(LeaseTask.class);

    protected final MetadataStore metadataStore;

    public ControllerTask(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }

    public abstract void process() throws ControllerException;

    @Override
    public void run() {
        String taskName = getClass().getSimpleName();
        LOGGER.debug("{} starts", taskName);
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            process();
        } catch (Throwable e) {
            LOGGER.error("Unexpected exception raised while execute {}", taskName, e);
        }
        LOGGER.debug("{} completed, costing {}ms", taskName, stopwatch.elapsed().toMillis());
    }
}
