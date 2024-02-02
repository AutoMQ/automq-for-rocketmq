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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

public class Futures {
    /**
     * Executes a loop using CompletableFutures, without invoking join()/get() on any of them or exclusively hogging a thread.
     *
     * @param condition A Supplier that indicates whether to proceed with the loop or not.
     * @param loopBody  A Supplier that returns a CompletableFuture which represents the body of the loop. This
     *                  supplier is invoked every time the loopBody needs to execute.
     * @param executor  An Executor that is used to execute the condition and the loop support code.
     * @return A CompletableFuture that, when completed, indicates the loop terminated without any exception. If
     * either the loopBody or condition throw/return Exceptions, these will be set as the result of this returned Future.
     */
    public static <T> CompletableFuture<T> loop(Supplier<Boolean> condition, Supplier<CompletableFuture<T>> loopBody,
        Executor executor) {
        CompletableFuture<T> result = new CompletableFuture<>();
        Loop<T> loop = new Loop<>(condition, loopBody, result, executor);
        executor.execute(loop);
        return result;
    }
}
