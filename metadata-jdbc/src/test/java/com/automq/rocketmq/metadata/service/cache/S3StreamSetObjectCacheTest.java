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

package com.automq.rocketmq.metadata.service.cache;

import com.automq.rocketmq.metadata.DatabaseTestBase;
import java.io.IOException;
import org.junit.jupiter.api.Test;

class S3StreamSetObjectCacheTest extends DatabaseTestBase {

    @Test
    public void testLoad() throws IOException {
        S3StreamSetObjectCache cache = new S3StreamSetObjectCache(getSessionFactory());
        cache.load(1);
    }
}