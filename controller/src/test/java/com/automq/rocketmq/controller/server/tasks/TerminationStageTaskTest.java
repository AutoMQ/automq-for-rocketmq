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

package com.automq.rocketmq.controller.server.tasks;

import apache.rocketmq.controller.v1.TerminateNodeReply;
import com.automq.rocketmq.common.PrefixThreadFactory;
import com.automq.rocketmq.controller.MetadataStore;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TerminationStageTaskTest {

    @Test
    public void testTask() {
        MetadataStore store = Mockito.mock(MetadataStore.class);
        StreamObserver<TerminateNodeReply> streamObserver = Mockito.mock(StreamObserver.class);
        Mockito.atLeast(1);
        TerminationStageTask task = new TerminationStageTask(store, Executors.newSingleThreadScheduledExecutor(new PrefixThreadFactory("test")),
            null, streamObserver);
        task.run();
    }

}