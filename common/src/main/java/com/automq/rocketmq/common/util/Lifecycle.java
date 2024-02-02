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

package com.automq.rocketmq.common.util;

/**
 * Control the lifecycle of a component or service.
 */
public interface Lifecycle {
    /**
     * Start the component.
     *
     * @throws Exception if the component fails to start
     */
    void start() throws Exception;

    /**
     * Shutdown the component.
     *
     * @throws Exception if the component fails to shut down
     */
    void shutdown() throws Exception;
}
