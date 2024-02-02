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

package com.automq.rocketmq.common.config;

import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.introspector.BeanAccess;

import static org.junit.jupiter.api.Assertions.assertEquals;

class YamlConfigTest {
    private static final String CONFIG_STR = """
        name: broker1
        proxy:
          name: proxy1
        store:
          maxFetchCount: 1
          maxFetchBytes: 2
          maxFetchTimeMillis: 3
        s3Stream:
          s3Region: us-east-1
          s3Bucket: bucket1
        """;

    @Test
    void load() {
        Yaml yaml = new Yaml();
        yaml.setBeanAccess(BeanAccess.FIELD);
        BrokerConfig config = yaml.loadAs(CONFIG_STR, BrokerConfig.class);
        assertEquals("broker1", config.name());
        assertEquals("proxy1", config.proxy().name());
        assertEquals(1, config.store().maxFetchCount());
        assertEquals(2, config.store().maxFetchBytes());
        assertEquals(3, config.store().maxFetchTimeMillis());
        assertEquals("us-east-1", config.s3Stream().s3Region());
        assertEquals("bucket1", config.s3Stream().s3Bucket());

        // Default value kept.
        assertEquals("/tmp/s3stream_wal", config.s3Stream().s3WALPath());

    }
}