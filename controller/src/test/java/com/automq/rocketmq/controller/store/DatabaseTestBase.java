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

package com.automq.rocketmq.controller.store;

import apache.rocketmq.controller.v1.AssignmentStatus;
import apache.rocketmq.controller.v1.GroupStatus;
import apache.rocketmq.controller.v1.GroupType;
import apache.rocketmq.controller.v1.SubStream;
import apache.rocketmq.controller.v1.SubStreams;
import apache.rocketmq.controller.v1.SubscriptionMode;
import com.automq.rocketmq.common.config.ControllerConfig;
import com.automq.rocketmq.controller.ControllerClient;
import com.automq.rocketmq.controller.MetadataStore;
import com.automq.rocketmq.metadata.dao.Group;
import com.automq.rocketmq.metadata.dao.Lease;
import com.automq.rocketmq.metadata.dao.QueueAssignment;
import com.automq.rocketmq.metadata.dao.S3ObjectCriteria;
import com.automq.rocketmq.metadata.dao.S3WalObject;
import com.automq.rocketmq.metadata.mapper.GroupMapper;
import com.automq.rocketmq.metadata.mapper.GroupProgressMapper;
import com.automq.rocketmq.metadata.mapper.LeaseMapper;
import com.automq.rocketmq.metadata.mapper.NodeMapper;
import com.automq.rocketmq.metadata.mapper.QueueAssignmentMapper;
import com.automq.rocketmq.metadata.mapper.RangeMapper;
import com.automq.rocketmq.metadata.mapper.S3ObjectMapper;
import com.automq.rocketmq.metadata.mapper.S3StreamObjectMapper;
import com.automq.rocketmq.metadata.mapper.S3WalObjectMapper;
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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mockito;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

public class DatabaseTestBase {

    protected ControllerConfig config;

    AtomicLong s3ObjectIdSequence;

    public DatabaseTestBase() {
        this.s3ObjectIdSequence = new AtomicLong(1);
        config = Mockito.spy(new TestControllerConfig());
        Mockito.doReturn(1L).when(config).scanIntervalInSecs();
        Mockito.doReturn(2).when(config).leaseLifeSpanInSecs();
    }

    protected ControllerClient getControllerClient() {
        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);
        return controllerClient;
    }

    protected QueueAssignment createAssignment(long topicId, int queueId, int srcNode, int dstNode, AssignmentStatus status)
        throws IOException {
        try (SqlSession session = getSessionFactory().openSession()) {
            QueueAssignmentMapper mapper = session.getMapper(QueueAssignmentMapper.class);
            QueueAssignment assignment = new QueueAssignment();
            assignment.setTopicId(topicId);
            assignment.setQueueId(queueId);
            assignment.setSrcNodeId(srcNode);
            assignment.setDstNodeId(dstNode);
            assignment.setStatus(status);
            mapper.create(assignment);
            session.commit();
            return assignment;
        }
    }

    protected Group createGroup(String name) throws IOException {
        try (SqlSession session = getSessionFactory().openSession()) {
            GroupMapper mapper = session.getMapper(GroupMapper.class);
            Group group = new Group();
            group.setName(name);
            group.setGroupType(GroupType.GROUP_TYPE_STANDARD);
            group.setSubMode(SubscriptionMode.SUB_MODE_POP);
            group.setMaxDeliveryAttempt(2);
            group.setDeadLetterTopicId(3L);
            group.setStatus(GroupStatus.GROUP_STATUS_ACTIVE);
            mapper.create(group);
            session.commit();
            return group;
        }
    }

    protected void awaitElectedAsLeader(MetadataStore metadataStore) {
        Awaitility.await()
            .with()
            .atMost(10, TimeUnit.SECONDS)
            .pollInterval(100, TimeUnit.MILLISECONDS).until(metadataStore::isLeader);
    }

    protected long nextS3ObjectId() {
        return this.s3ObjectIdSequence.getAndIncrement();
    }

    protected static MySQLContainer mySQLContainer = new MySQLContainer<>(DockerImageName.parse("mysql:8"))
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
            session.getMapper(S3WalObjectMapper.class).delete(null, null, null);

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

    protected List<S3WalObject> buildS3WalObjs(long objectId, int count) {
        List<S3WalObject> s3StreamObjects = new ArrayList<>();

        for (long i = 0; i < count; i++) {
            S3WalObject s3StreamObject = new S3WalObject();
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
