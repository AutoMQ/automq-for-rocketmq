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

package com.automq.rocketmq.proxy;

import com.automq.rocketmq.proxy.model.ProxyContextExt;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.opentelemetry.instrumentation.annotations.SpanAttribute;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import io.opentelemetry.instrumentation.api.annotation.support.SpanAttributesExtractor;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.IntStream;
import org.apache.commons.lang3.StringUtils;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;

@Aspect
public class ProxyTraceAspect {
    ConcurrentMap<Method, String[]> methodParameterNamesMap = new ConcurrentHashMap<>();

    SpanAttributesExtractor extractor = SpanAttributesExtractor.create(
        (method, parameters) -> IntStream.range(0, parameters.length)
            .mapToObj(i -> {
                Parameter parameter = parameters[i];
                SpanAttribute parameterAnnotation = parameter.getAnnotation(SpanAttribute.class);
                if (parameterAnnotation == null) {
                    return "";
                }

                String[] parameterNames = methodParameterNamesMap.get(method);
                if (parameterNames == null || parameterNames.length <= i) {
                    return "";
                }

                return parameterNames[i];
            })
            .toArray(String[]::new)
    );

    @Pointcut("@annotation(withSpan)")
    public void trace(WithSpan withSpan) {
    }

    @Around(value = "trace(withSpan) && execution(* *(..))", argNames = "joinPoint,withSpan")
    public Object createSpan(ProceedingJoinPoint joinPoint, WithSpan withSpan) throws Throwable {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();
        methodParameterNamesMap.putIfAbsent(method, signature.getParameterNames());
        Object[] args = joinPoint.getArgs();

        Span span;
        Scope scope;
        if (args.length > 0 && args[0] instanceof ProxyContextExt context) {
            Tracer tracer = context.getTracer();
            String spanName = StringUtils.isEmpty(withSpan.value()) ? method.getName() : withSpan.value();
            span = tracer.spanBuilder(spanName).setSpanKind(SpanKind.SERVER).startSpan();
            scope = span.makeCurrent();
            Attributes attributes = extractor.extract(method, args);
            span.setAllAttributes(attributes);
        } else {
            span = null;
            scope = null;
        }

        Object result;
        try {
            result = joinPoint.proceed();
        } catch (Throwable t) {
            if (span != null) {
                span.recordException(t);
                span.setStatus(StatusCode.ERROR, t.getMessage());
                scope.close();
                span.end();
            }
            throw t;
        }

        if (span != null) {
            if (method.getReturnType() == CompletableFuture.class) {
                CompletableFuture<Object> future = (CompletableFuture<Object>) result;
                future.whenComplete((r, t) -> {
                    if (t != null) {
                        span.recordException(t);
                        span.setStatus(StatusCode.ERROR, t.getMessage());
                    } else {
                        span.setStatus(StatusCode.OK);
                    }
                    scope.close();
                    span.end();
                });
            } else {
                span.setStatus(StatusCode.OK);
                scope.close();
                span.end();
            }
        }

        return result;
    }
}
