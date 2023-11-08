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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Loop<T> implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Loop.class);

    final Supplier<Boolean> condition;
    final Supplier<CompletableFuture<T>> loopBody;

    /**
     * A CompletableFuture that will be completed, whether normally or exceptionally, when the loop completes.
     */
    final CompletableFuture<Void> result;


    final Executor executor;

    public Loop(Supplier<Boolean> condition, Supplier<CompletableFuture<T>> loopBody, CompletableFuture<Void> result,
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

    void execute() {
        if (this.condition.get()) {
            this.loopBody.get()
                .exceptionally(e -> {
                    LOGGER.error("Unexpected exception raised", e);
                    return null;
                }).thenRunAsync(this, executor);
        } else {
            result.complete(null);
            LOGGER.debug("Loop completed");
        }
    }
}
