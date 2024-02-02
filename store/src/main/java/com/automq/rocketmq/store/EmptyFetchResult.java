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

package com.automq.rocketmq.store;

import com.automq.stream.api.FetchResult;
import com.automq.stream.api.RecordBatchWithContext;
import java.util.ArrayList;
import java.util.List;

public class EmptyFetchResult implements FetchResult {
    @Override
    public List<RecordBatchWithContext> recordBatchList() {
        return new ArrayList<>();
    }
}
