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

package com.automq.rocketmq.metadata;

import apache.rocketmq.controller.v1.SubStream;
import apache.rocketmq.controller.v1.SubStreams;
import com.automq.rocketmq.metadata.dao.Lease;
import com.automq.rocketmq.metadata.dao.S3ObjectCriteria;
import com.automq.rocketmq.metadata.dao.S3StreamSetObject;
import com.automq.rocketmq.metadata.mapper.GroupMapper;
import com.automq.rocketmq.metadata.mapper.GroupProgressMapper;
import com.automq.rocketmq.metadata.mapper.LeaseMapper;
import com.automq.rocketmq.metadata.mapper.NodeMapper;
import com.automq.rocketmq.metadata.mapper.QueueAssignmentMapper;
import com.automq.rocketmq.metadata.mapper.RangeMapper;
import com.automq.rocketmq.metadata.mapper.S3ObjectMapper;
import com.automq.rocketmq.metadata.mapper.S3StreamObjectMapper;
import com.automq.rocketmq.metadata.mapper.S3StreamSetObjectMapper;
import com.automq.rocketmq.metadata.mapper.StreamMapper;
import com.automq.rocketmq.metadata.mapper.TopicMapper;

import com.automq.rocketmq.metadata.dao.S3StreamObject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

public class DatabaseTestBase {


    AtomicLong s3ObjectIdSequence;

    public DatabaseTestBase() {
        this.s3ObjectIdSequence = new AtomicLong(1);
    }

    protected long nextS3ObjectId() {
        return this.s3ObjectIdSequence.getAndIncrement();
    }

    static MySQLContainer mySQLContainer = new MySQLContainer<>(DockerImageName.parse("mysql:8"))
        .withDatabaseName("metadata")
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
            session.getMapper(QueueAssignmentMapper.class).delete(null);
            session.getMapper(TopicMapper.class).delete(null);
            session.getMapper(StreamMapper.class).delete(null);
            session.getMapper(RangeMapper.class).delete(null, null);
            session.getMapper(S3ObjectMapper.class).deleteByCriteria(S3ObjectCriteria.newBuilder().build());
            session.getMapper(S3StreamObjectMapper.class).delete(null, null, null);
            session.getMapper(S3StreamSetObjectMapper.class).delete(null, null, null);

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

    protected String toJson(Map<Long, SubStream> map) {
        SubStreams subStreams = SubStreams.newBuilder().putAllSubStreams(map).build();
        try {
            return JsonFormat.printer().print(subStreams);
        } catch (InvalidProtocolBufferException e) {
            Assertions.fail(e);
            throw new RuntimeException(e);
        }
    }

    protected List<S3StreamObject> buildS3StreamObjs(long objectId,
        int count, long startOffset, long interval) {
        List<S3StreamObject> s3StreamObjects = new ArrayList<>();

        for (long i = 0; i < count; i++) {
            S3StreamObject s3StreamObject = new S3StreamObject();
            s3StreamObject.setObjectId(objectId + i);
            s3StreamObject.setObjectSize(100 + i);
            s3StreamObject.setStreamId(i + 1);
            s3StreamObject.setStartOffset(startOffset + i * interval);
            s3StreamObject.setEndOffset(startOffset + (i + 1) * interval);
            s3StreamObject.setBaseDataTimestamp(new Date());
            s3StreamObjects.add(s3StreamObject);
        }

        return s3StreamObjects;
    }

    protected List<S3StreamSetObject> buildS3WalObjs(long objectId, int count) {
        List<S3StreamSetObject> s3StreamObjects = new ArrayList<>();

        for (long i = 0; i < count; i++) {
            S3StreamSetObject s3StreamObject = new S3StreamSetObject();
            s3StreamObject.setObjectId(objectId + i);
            s3StreamObject.setObjectSize(100 + i);
            s3StreamObject.setSequenceId(objectId + i);
            s3StreamObject.setNodeId((int) i + 1);
            s3StreamObject.setBaseDataTimestamp(new Date());
            s3StreamObjects.add(s3StreamObject);
        }

        return s3StreamObjects;
    }

    protected Map<Long, SubStream> buildWalSubStreams(int count, long startOffset, long interval) {
        Map<Long, SubStream> subStreams = new HashMap<>();
        for (int i = 0; i < count; i++) {
            SubStream subStream = SubStream.newBuilder()
                .setStreamId(i + 1)
                .setStartOffset(startOffset + i * interval)
                .setEndOffset(startOffset + (i + 1) * interval)
                .build();

            subStreams.put((long) i + 1, subStream);
        }
        return subStreams;
    }
}
