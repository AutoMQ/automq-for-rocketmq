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

package com.automq.rocketmq.controller.tasks;

import com.automq.rocketmq.controller.exception.ControllerException;
import com.automq.rocketmq.controller.metadata.MetadataStore;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ControllerTask implements Runnable {
    protected static final Logger LOGGER = LoggerFactory.getLogger(LeaseTask.class);

    protected final MetadataStore metadataStore;

    public ControllerTask(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }

    public abstract void process() throws ControllerException;

    @Override
    public void run() {
        String taskName = getClass().getSimpleName();
        LOGGER.debug("{} starts", taskName);
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            process();
        } catch (Throwable e) {
            LOGGER.error("Unexpected exception raised while execute {}", taskName, e);
        }
        LOGGER.debug("{} completed, costing {}ms", taskName, stopwatch.elapsed().toMillis());
    }
}
