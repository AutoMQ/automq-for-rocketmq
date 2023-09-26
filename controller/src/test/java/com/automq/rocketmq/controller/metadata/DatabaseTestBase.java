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

package com.automq.rocketmq.controller.metadata;

import com.automq.rocketmq.controller.metadata.database.dao.Lease;
import com.automq.rocketmq.controller.metadata.database.mapper.GroupMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.GroupProgressMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.LeaseMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.NodeMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.QueueAssignmentMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.RangeMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.S3ObjectMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.S3StreamObjectMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.S3WALObjectMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.StreamAffiliationMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.StreamMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.TopicMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.Calendar;
import java.util.Properties;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

public class DatabaseTestBase {

    static MySQLContainer mySQLContainer = new MySQLContainer<>(DockerImageName.parse("mysql:8"))
        .withInitScript("ddl.sql")
        .withReuse(true);

    @BeforeAll
    public static void startMySQLContainer() {
        mySQLContainer.start();
    }

    protected SqlSessionFactory getSessionFactory() throws IOException {
        String resource = "database/mybatis-config.xml";
        InputStream inputStream = Resources.getResourceAsStream(resource);

        Properties properties = new Properties();
        properties.put("password", "test");
        properties.put("jdbcUrl", mySQLContainer.getJdbcUrl() + "?TC_REUSABLE=true");
        return new SqlSessionFactoryBuilder().build(inputStream, properties);
    }

    @BeforeEach
    protected void cleanTables() throws IOException {
        try (SqlSession session = getSessionFactory().openSession(true)) {
            session.getMapper(GroupMapper.class).delete(null);
            session.getMapper(GroupProgressMapper.class).delete(null, null);
            session.getMapper(NodeMapper.class).delete(null);
            session.getMapper(StreamAffiliationMapper.class).delete(null, null);
            session.getMapper(QueueAssignmentMapper.class).delete(null);
            session.getMapper(TopicMapper.class).delete(null);
            session.getMapper(StreamMapper.class).delete(null);
            session.getMapper(RangeMapper.class).delete(null, null);
            session.getMapper(S3ObjectMapper.class).deleteDangerous();
            session.getMapper(S3StreamObjectMapper.class).delete(null, null, null);
            session.getMapper(S3WALObjectMapper.class).delete(null, null, null);


            LeaseMapper mapper = session.getMapper(LeaseMapper.class);
            Lease lease = mapper.currentWithWriteLock();
            lease.setNodeId(1);
            lease.setEpoch(1);
            Calendar calendar = Calendar.getInstance();
            calendar.set(2023, Calendar.JANUARY, 1);
            lease.setExpirationTime(calendar.getTime());
            mapper.update(lease);
        }
    }
}
