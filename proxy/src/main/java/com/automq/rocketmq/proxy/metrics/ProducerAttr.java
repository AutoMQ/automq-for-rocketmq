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

public class ProducerAttr {
    LanguageCode language;
    int version;

    public ProducerAttr(LanguageCode language, int version) {
        this.language = language;
        this.version = version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ProducerAttr attr = (ProducerAttr) o;
        return version == attr.version && language == attr.language;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(language, version);
    }
}
