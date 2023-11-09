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
}
