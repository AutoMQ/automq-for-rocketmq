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

package com.automq.rocketmq.proxy.model;

import com.automq.rocketmq.common.trace.TraceContext;
import com.automq.rocketmq.common.trace.TraceHelper;
import com.google.common.base.Stopwatch;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import java.util.Optional;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import org.apache.rocketmq.proxy.common.ProxyContext;

public class ProxyContextExt extends ProxyContext implements TraceContext {
    private final Stopwatch stopwatch = Stopwatch.createStarted();
    public static final long DEFAULT_TIMEOUT_MILLIS = 3000;
    private boolean suspended;
    private boolean relayed;

    private final Tracer tracer;
    private Span rootSpan;

    private final BlockingDeque<Span> spanStack = new LinkedBlockingDeque<>();

    private ProxyContextExt() {
        super();
        tracer = TraceHelper.getTracer();
    }

    public static ProxyContextExt create() {
        return new ProxyContextExt();
    }

    public static ProxyContextExt create(ProxyContext context) {
        ProxyContextExt contextExt = new ProxyContextExt();
        contextExt.getValue().putAll(context.getValue());
        return contextExt;
    }

    public boolean suspended() {
        return suspended;
    }

    public void setSuspended(boolean suspended) {
        this.suspended = suspended;
    }

    public boolean relayed() {
        return relayed;
    }

    public void setRelayed(boolean relayed) {
        this.relayed = relayed;
    }

    public long getElapsedTimeNanos() {
        return stopwatch.elapsed().toNanos();
    }

    @Override
    public Long getRemainingMs() {
        return super.getRemainingMs() == null ? DEFAULT_TIMEOUT_MILLIS : super.getRemainingMs();
    }

    public Span rootSpan() {
        return rootSpan;
    }

    public BlockingDeque<Span> spanStack() {
        return spanStack;
    }

    @Override
    public Optional<Tracer> tracer() {
        return Optional.of(tracer);
    }

    @Override
    public Optional<Span> span() {
        return Optional.ofNullable(spanStack.peek());
    }

    @Override
    public void attachSpan(Span span) {
        if (spanStack.isEmpty()) {
            rootSpan = span;
        }
        spanStack.push(span);
    }

    @Override
    public void detachSpan() {
        if (!spanStack.isEmpty()) {
            Span span = spanStack.pop();
            span.end();
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
