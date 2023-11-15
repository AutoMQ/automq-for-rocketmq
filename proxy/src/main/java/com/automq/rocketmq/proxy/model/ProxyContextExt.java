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

    private final Tracer tracer;

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

    public long getElapsedTimeNanos() {
        return stopwatch.elapsed().toNanos();
    }

    @Override
    public Long getRemainingMs() {
        return super.getRemainingMs() == null ? DEFAULT_TIMEOUT_MILLIS : super.getRemainingMs();
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
