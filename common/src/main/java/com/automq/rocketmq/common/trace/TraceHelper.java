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

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.TracerProvider;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;

public class TraceHelper {
    private static final SpanAttributesExtractor EXTRACTOR = SpanAttributesExtractor.create();

    public static Tracer getTracer() {
        TracerProvider tracerProvider = GlobalOpenTelemetry.getTracerProvider();
        return tracerProvider.get("automq-for-rocketmq");
    }

    public static Optional<Span> createAndStartSpan(TraceContext context, String name, SpanKind kind) {
        Optional<Tracer> tracer = context.tracer();
        if (tracer.isEmpty()) {
            return Optional.empty();
        }

        Context spanContext = Context.current();
        Optional<Span> parentSpan = context.span();
        if (parentSpan.isPresent() && parentSpan.get().isRecording()) {
            spanContext = spanContext.with(parentSpan.get());
        }

        Span span = tracer.get().spanBuilder(name)
            .setParent(spanContext)
            .setSpanKind(kind)
            .startSpan();
        context.attachSpan(span);

        return Optional.of(span);
    }

    public static void endSpan(TraceContext context, Span span) {
        span.setStatus(StatusCode.OK);
        span.end();
        context.detachSpan();
    }

    public static void endSpan(TraceContext context, Span span, Throwable throwable) {
        if (throwable == null) {
            endSpan(context, span);
            return;
        }

        if (throwable instanceof CompletionException || throwable instanceof ExecutionException) {
            if (throwable.getCause() != null) {
                throwable = throwable.getCause();
            }
        }

        if (throwable instanceof TimeoutException) {
            context.detachAllSpan();
            span.recordException(throwable);
            span.setStatus(StatusCode.ERROR, throwable.getMessage());
        } else {
            span.recordException(throwable);
            span.setStatus(StatusCode.ERROR, throwable.getMessage());
            span.end();
            context.detachSpan();
        }
    }

    public static Object doTrace(ProceedingJoinPoint joinPoint, TraceContext context,
        WithSpan withSpan) throws Throwable {
        return doTrace(joinPoint, context, withSpan, Map.of());
    }

    public static Object doTrace(ProceedingJoinPoint joinPoint, TraceContext context, WithSpan withSpan,
        Map<String, String> attributeMap) throws Throwable {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();
        Object[] args = joinPoint.getArgs();

        Optional<Tracer> tracer = context.tracer();
        if (tracer.isEmpty()) {
            return joinPoint.proceed();
        }

        Context spanContext = Context.current();
        Optional<Span> parentSpan = context.span();
        if (parentSpan.isPresent() && parentSpan.get().isRecording()) {
            spanContext = spanContext.with(parentSpan.get());
        }

        String className = method.getDeclaringClass().getSimpleName();
        String spanName = withSpan.value().isEmpty() ? className + "::" + method.getName() : withSpan.value();

        Span span = tracer.get().spanBuilder(spanName)
            .setParent(spanContext)
            .setSpanKind(withSpan.kind())
            .startSpan();
        context.attachSpan(span);

        Attributes attributes = EXTRACTOR.extract(method, signature.getParameterNames(), args);
        span.setAllAttributes(attributes);
        attributeMap.forEach(span::setAttribute);

        try {
            if (method.getReturnType() == CompletableFuture.class) {
                return doTraceWhenReturnCompletableFuture(context, span, joinPoint);
            } else {
                return doTraceWhenReturnObject(context, span, joinPoint);
            }
        } catch (Throwable t) {
            span.recordException(t);
            span.setStatus(StatusCode.ERROR, t.getMessage());
            context.detachSpan();
            throw t;
        }
    }

    private static CompletableFuture<?> doTraceWhenReturnCompletableFuture(TraceContext context, Span span,
        ProceedingJoinPoint joinPoint) throws Throwable {
        CompletableFuture<?> future = (CompletableFuture<?>) joinPoint.proceed();
        return future.whenComplete((r, t) -> endSpan(context, span, t));
    }

    private static Object doTraceWhenReturnObject(TraceContext context, Span span,
        ProceedingJoinPoint joinPoint) throws Throwable {
        Object result = joinPoint.proceed();
        endSpan(context, span);
        return result;
    }
}
