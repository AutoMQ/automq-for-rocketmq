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

import com.automq.rocketmq.common.config.ControllerConfig;
import com.automq.rocketmq.controller.MetadataStore;
import com.automq.rocketmq.controller.client.GrpcControllerClient;
import com.automq.rocketmq.controller.server.store.DefaultMetadataStore;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

public class MetadataStoreBuilder {
    public static ControllerServiceImpl build(MetadataStore metadataStore) {
        return new ControllerServiceImpl(metadataStore);
    }
    public static MetadataStore build(ControllerConfig config) throws IOException {
        SqlSessionFactory sessionFactory = getSessionFactory(config.dbUrl(), config.dbUserName(), config.dbPassword());
        return new DefaultMetadataStore(new GrpcControllerClient(config), sessionFactory, config);
    }

    private static SqlSessionFactory getSessionFactory(String dbUrl, String dbUser, String dbPassword) throws IOException {
        String resource = "database/mybatis-config.xml";
        InputStream inputStream = Resources.getResourceAsStream(resource);

        Properties properties = new Properties();
        properties.put("userName", dbUser);
        properties.put("password", dbPassword);
        properties.put("jdbcUrl", dbUrl);
        return new SqlSessionFactoryBuilder().build(inputStream, properties);
    }
}
