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

package com.automq.rocketmq.controller.server.store.impl.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class Inflight<T> {

    private final AtomicBoolean completed;
    private final List<CompletableFuture<T>> futures;

    public Inflight() {
        completed = new AtomicBoolean(false);
        futures = new ArrayList<>();
    }

    public synchronized boolean addFuture(CompletableFuture<T> future) {
        if (!completed.get()) {
            this.futures.add(future);
            return true;
        }
        return false;
    }

    public void complete(T value) {
        if (completed.compareAndSet(false, true)) {
            futures.forEach(future -> {
                if (!future.isDone()) {
                    future.complete(value);
                }
            });
        }
    }

    public void completeExceptionally(Throwable e) {
        if (completed.compareAndSet(false, true)) {
            futures.forEach(future -> {
                if (!future.isDone()) {
                    future.completeExceptionally(e);
                }
            });
        }
    }
}
