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

import com.automq.rocketmq.controller.MetadataStore;
import java.util.Date;

public abstract class ScanTask extends ControllerTask {

    protected Date lastScanTime;

    public ScanTask(MetadataStore metadataStore) {
        super(metadataStore);
    }
}
