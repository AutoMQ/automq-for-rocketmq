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

package com.automq.rocketmq.common.trace;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import java.util.Optional;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * TraceContext is used to prorate context for tracing.
 * It can be sent to another thread, but it should not be used concurrently.
 * If there is a need to do some jobs parallel, please create a new {@link TraceContext} for each task.
 */
@NotThreadSafe
public interface TraceContext {
    /**
     * Get the tracer to create span.
     *
     * @return the tracer
     */
    Optional<Tracer> tracer();

    /**
     * Get the current span.
     *
     * @return the current span
     */
    Optional<Span> span();

    /**
     * Attach a span to the context which will be the current span.
     *
     * @param span the span to attach
     */
    void attachSpan(Span span);

    /**
     * Remove the current span from the context.
     */
    void detachSpan();

    /**
     * Remove the current span from the context.
     */
    void detachAllSpan();
}
