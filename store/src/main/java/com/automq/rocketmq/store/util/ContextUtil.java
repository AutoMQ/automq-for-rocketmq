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

package com.automq.rocketmq.store.util;

import com.automq.stream.s3.trace.context.TraceContext;
import io.opentelemetry.context.Context;

public class ContextUtil {
    public static TraceContext buildStreamTraceContext(com.automq.rocketmq.common.trace.TraceContext context) {
        boolean isTraceEnabled = context.tracer().isPresent();
        Context currContext = context.span().map(span -> Context.current().with(span)).orElse(Context.current());
        return new TraceContext(isTraceEnabled, context.tracer().get(), currContext);
    }
}
