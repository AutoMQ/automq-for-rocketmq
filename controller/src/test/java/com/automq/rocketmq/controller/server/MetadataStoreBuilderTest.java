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

package com.automq.rocketmq.controller.server;

import com.automq.rocketmq.controller.MetadataStore;
import com.automq.rocketmq.controller.store.DatabaseTestBase;
import java.io.IOException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class MetadataStoreBuilderTest extends DatabaseTestBase {

    @Test
    public void testBuild() throws IOException {
        Mockito.when(config.dbUserName()).thenReturn(DatabaseTestBase.mySQLContainer.getUsername());
        Mockito.when(config.dbPassword()).thenReturn(DatabaseTestBase.mySQLContainer.getPassword());
        Mockito.when(config.dbUrl()).thenReturn(DatabaseTestBase.mySQLContainer.getJdbcUrl());
        try (MetadataStore store = MetadataStoreBuilder.build(config)) {
            Assertions.assertNotNull(store);
        }
    }

}