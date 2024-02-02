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

package com.automq.rocketmq.store.model;

import com.automq.rocketmq.common.trace.TraceContext;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import java.util.Optional;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

public class StoreContext implements TraceContext {
    public static final StoreContext EMPTY = new StoreContext("", "", null);

    private final String topic;
    private final String consumerGroup;
    private final Tracer tracer;
    private BlockingDeque<Span> spanStack = new LinkedBlockingDeque<>();

    public StoreContext(String topic, String consumerGroup, Tracer tracer) {
        this.topic = topic;
        this.consumerGroup = consumerGroup;
        this.tracer = tracer;
    }

    public void shareSpanStack(BlockingDeque<Span> spanStack) {
        this.spanStack = spanStack;
    }

    @Override
    public Optional<Tracer> tracer() {
        return Optional.ofNullable(tracer);
    }

    @Override
    public Optional<Span> span() {
        return Optional.ofNullable(spanStack.peek());
    }

    @Override
    public void attachSpan(Span span) {
        spanStack.push(span);
    }

    @Override
    public void detachSpan() {
        if (!spanStack.isEmpty()) {
            spanStack.pop();
        }
    }

    @Override
    public void detachAllSpan() {
        while (!spanStack.isEmpty()) {
            Span span = spanStack.pop();
            span.setStatus(StatusCode.ERROR, "Span is closed due to timeout");
            span.end();
        }
    }
}
