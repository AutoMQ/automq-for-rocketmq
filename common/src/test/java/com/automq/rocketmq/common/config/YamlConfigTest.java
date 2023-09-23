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

package com.automq.rocketmq.common.config;

import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.introspector.BeanAccess;

import static org.junit.jupiter.api.Assertions.assertEquals;

class YamlConfigTest {
    private static final String CONFIG_STR = """
        broker:
          name: broker1
        proxy:
          name: proxy1
        store:
          maxFetchCount: 1
          maxFetchBytes: 2
          maxFetchTimeNanos: 3
        s3Stream:
          s3Region: us-east-1
          s3Bucket: bucket1
        """;

    @Test
    void load() {
        Yaml yaml = new Yaml();
        yaml.setBeanAccess(BeanAccess.FIELD);
        YamlConfig config = yaml.loadAs(CONFIG_STR, YamlConfig.class);
        assertEquals("broker1", config.broker().name());
        assertEquals("proxy1", config.proxy().name());
        assertEquals(1, config.store().maxFetchCount());
        assertEquals(2, config.store().maxFetchBytes());
        assertEquals(3, config.store().maxFetchTimeNanos());
        assertEquals("us-east-1", config.s3Stream().s3Region());
        assertEquals("bucket1", config.s3Stream().s3Bucket());
    }
}