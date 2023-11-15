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

package com.automq.rocketmq.proxy.grpc.activity;

import apache.rocketmq.v2.QueryRouteResponse;
import com.automq.rocketmq.proxy.model.ProxyContextExt;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.proxy.grpc.v2.route.RouteActivity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;

class ExtendRouteActivityTest {
    private ExtendRouteActivity routeActivity;

    @BeforeEach
    void setUp() {
        routeActivity = Mockito.mock(ExtendRouteActivity.class, Answers.CALLS_REAL_METHODS);
        Mockito.doReturn(CompletableFuture.completedFuture(QueryRouteResponse.newBuilder().build()))
            .when((RouteActivity) routeActivity)
            .queryRoute(any(), any());
    }

    @Test
    void queryRoute() {
        routeActivity.queryRoute(ProxyContextExt.create(), null);
    }

    @Test
    void queryAssignment() {
    }
}