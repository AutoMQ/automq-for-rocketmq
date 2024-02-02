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

package com.automq.rocketmq.store.model.message;

import com.automq.rocketmq.common.model.FlatMessageExt;
import java.util.List;

public record SQLFilter(String expression) implements Filter {
    @Override
    public FilterType type() {
        return FilterType.SQL;
    }

    @Override
    public List<FlatMessageExt> doFilter(List<FlatMessageExt> messageList) {
        return messageList;
    }

    @Override
    public boolean doFilter(String tag) {
        return true;
    }
}
