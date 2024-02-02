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
package com.automq.rocketmq.proxy.metrics;

import com.google.common.base.Objects;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;

public class ConsumerAttr {
    String group;
    LanguageCode language;
    int version;
    ConsumeType consumeMode;

    public ConsumerAttr(String group, LanguageCode language, int version, ConsumeType consumeMode) {
        this.group = group;
        this.language = language;
        this.version = version;
        this.consumeMode = consumeMode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ConsumerAttr attr = (ConsumerAttr) o;
        return version == attr.version && Objects.equal(group, attr.group) && language == attr.language && consumeMode == attr.consumeMode;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(group, language, version, consumeMode);
    }
}
