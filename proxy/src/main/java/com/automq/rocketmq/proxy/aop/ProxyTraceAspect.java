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
