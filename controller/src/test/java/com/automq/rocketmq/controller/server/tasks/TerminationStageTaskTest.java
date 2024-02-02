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