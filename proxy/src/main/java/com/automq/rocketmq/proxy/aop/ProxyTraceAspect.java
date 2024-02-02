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

package com.automq.rocketmq.proxy.aop;

import com.automq.rocketmq.common.trace.TraceContext;
import com.automq.rocketmq.common.trace.TraceHelper;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.Map;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;

@Aspect
public class ProxyTraceAspect {

    @Pointcut("@annotation(withSpan)")
    public void trace(WithSpan withSpan) {
    }

    @Around(value = "trace(withSpan) && execution(* *(..))", argNames = "joinPoint,withSpan")
    public Object createSpan(ProceedingJoinPoint joinPoint, WithSpan withSpan) throws Throwable {
        Object[] args = joinPoint.getArgs();
        if (args.length > 0 && args[0] instanceof TraceContext context) {
            return TraceHelper.doTrace(joinPoint, context, withSpan, Map.of("module", "proxy"));
        }

        return joinPoint.proceed();
    }
}
