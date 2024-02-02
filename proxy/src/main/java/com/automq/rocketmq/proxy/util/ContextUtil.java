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

package com.automq.rocketmq.proxy.util;

import com.automq.rocketmq.proxy.model.ProxyContextExt;
import com.automq.rocketmq.store.model.StoreContext;
import org.apache.rocketmq.proxy.common.ProxyContext;

public class ContextUtil {
    public static StoreContext buildStoreContext(ProxyContext context, String topic, String consumerGroup) {
        if (context instanceof ProxyContextExt contextExt) {
            StoreContext storeContext = new StoreContext(topic, consumerGroup, contextExt.tracer().orElse(null));
            storeContext.shareSpanStack(contextExt.spanStack());
            return storeContext;
        }
        return new StoreContext(topic, consumerGroup, null);
    }
}
