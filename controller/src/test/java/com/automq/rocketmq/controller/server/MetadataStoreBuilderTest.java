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