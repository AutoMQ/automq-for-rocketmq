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

package com.automq.rocketmq.controller;

import com.automq.rocketmq.common.config.ControllerConfig;
import com.automq.rocketmq.controller.metadata.GrpcControllerClient;
import com.automq.rocketmq.controller.metadata.MetadataStore;
import com.automq.rocketmq.controller.metadata.database.DefaultMetadataStore;
import com.automq.rocketmq.controller.metadata.database.dao.Node;
import org.apache.ibatis.datasource.pooled.PooledDataSource;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;

public class MetadataStoreBuilder {
    public static MetadataStore build(ControllerConfig config, Node node) {
        PooledDataSource dataSource = new PooledDataSource("com.mysql.jdbc.Driver", config.dbUrl(),
            config.dbUser(), config.dbPassword());

        // Build SqlSessionFactory
        Environment environment = new Environment("default", new JdbcTransactionFactory(), dataSource);
        Configuration configuration = new Configuration(environment);

        SqlSessionFactory sessionFactory = new SqlSessionFactoryBuilder().build(configuration);

        // TODO: Should unify the config interface.
        return new DefaultMetadataStore(new GrpcControllerClient(), sessionFactory, new com.automq.rocketmq.controller.metadata.ControllerConfig() {
            @Override
            public int nodeId() {
                return node.getId();
            }

            @Override
            public long epoch() {
                return node.getEpoch();
            }

            @Override
            public int scanIntervalInSecs() {
                return config.scanIntervalInSecs();
            }

            @Override
            public int leaseLifeSpanInSecs() {
                return config.leaseLifeSpanInSecs();
            }

            @Override
            public int nodeAliveIntervalInSecs() {
                return config.nodeAliveIntervalInSecs();
            }
        });
    }
}
