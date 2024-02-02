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

package com.automq.rocketmq.common;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;

public class PrefixThreadFactory implements ThreadFactory {
    private final String prefix;

    private final AtomicInteger index;

    public PrefixThreadFactory(String prefix) {
        this.prefix = prefix;
        this.index = new AtomicInteger(0);
    }

    @Override
    public Thread newThread(@Nonnull Runnable r) {
        final String threadName = String.format("%s_%d", this.prefix, this.index.getAndIncrement());
        Thread t = new Thread(r);
        t.setName(threadName);
        t.setDaemon(true);
        return t;
    }
}
