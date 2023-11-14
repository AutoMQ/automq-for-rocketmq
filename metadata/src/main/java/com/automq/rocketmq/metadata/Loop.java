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
