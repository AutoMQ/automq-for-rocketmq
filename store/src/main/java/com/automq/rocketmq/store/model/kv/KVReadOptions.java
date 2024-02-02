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

package com.automq.rocketmq.store.model.kv;

public class KVReadOptions {

    private long snapshotVersion = -1;

    public void setSnapshotVersion(long snapshotVersion) {
        this.snapshotVersion = snapshotVersion;
    }

    public boolean isSnapshotRead() {
        return snapshotVersion != -1;
    }

    public long getSnapshotVersion() {
        return snapshotVersion;
    }
}
