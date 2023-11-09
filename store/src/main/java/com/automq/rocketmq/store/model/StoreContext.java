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

package com.automq.rocketmq.store.model;

import com.automq.rocketmq.common.trace.TraceContext;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import java.util.Optional;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

public class StoreContext implements TraceContext {
    public static final StoreContext EMPTY = new StoreContext("", "", null);

    private final String topic;
    private final String consumerGroup;
    private final Tracer tracer;
    private final BlockingDeque<Span> spanStack = new LinkedBlockingDeque<>();

    public StoreContext(String topic, String consumerGroup, Tracer tracer) {
        this.topic = topic;
        this.consumerGroup = consumerGroup;
        this.tracer = tracer;
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
        spanStack.pop();
    }
}
