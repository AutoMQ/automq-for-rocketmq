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

package com.automq.rocketmq.metadata;

import apache.rocketmq.common.v1.Code;
import com.automq.rocketmq.common.exception.ControllerException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Loop<T> implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Loop.class);

    final Supplier<Boolean> condition;

    final Supplier<CompletableFuture<T>> loopBody;

    /**
     * A CompletableFuture that will be completed when the loop completes.
     */
    final CompletableFuture<T> result;

    final Executor executor;

    private boolean terminated;

    public Loop(Supplier<Boolean> condition, Supplier<CompletableFuture<T>> loopBody, CompletableFuture<T> result,
        Executor executor) {
        this.condition = condition;
        this.loopBody = loopBody;
        this.result = result;
        this.executor = executor;
    }

    @Override
    public void run() {
        execute();
    }

    boolean completed() {
        if (null == result) {
            return true;
        }

        return result.isDone() || result.isCancelled();
    }

    void execute() {
        if (terminated) {
            LOGGER.debug("Loop has terminated");
            return;
        }

        if (completed()) {
            LOGGER.debug("Loop has completed");
            return;
        }

        if (this.condition.get()) {
            this.loopBody.get()
                .exceptionally(e -> {
                    if (e.getCause() instanceof ControllerException ex) {
                        if (ex.getErrorCode() != Code.INTERNAL_VALUE && ex.getErrorCode() != Code.MOCK_FAILURE_VALUE) {
                            terminated = true;
                            result.completeExceptionally(e);
                            throw new CompletionException(e);
                        }
                    }
                    LOGGER.error("Unexpected exception raised", e);
                    return null;
                }).thenApply(t -> {
                    if (!completed()) {
                        result.complete(t);
                    }
                    return t;
                }).thenRunAsync(this, executor);
        } else {
            LOGGER.debug("Loop completed");
        }
    }
}
