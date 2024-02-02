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
