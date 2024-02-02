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

import com.automq.rocketmq.metadata.mapper.S3ObjectMapper;
import com.automq.rocketmq.metadata.mapper.SequenceMapper;
import java.io.IOException;
import java.util.UUID;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SequenceTest extends DatabaseTestBase {

    @Test
    public void testCRUD() throws IOException {
        try (SqlSession session = getSessionFactory().openSession()) {
            SequenceMapper sequenceMapper = session.getMapper(SequenceMapper.class);
            long next = sequenceMapper.next(S3ObjectMapper.SEQUENCE_NAME);
            Assertions.assertTrue(next >= 1);

            String name = UUID.randomUUID().toString();
            long initial = 100;
            sequenceMapper.create(name, initial);
            next = sequenceMapper.next(name);
            Assertions.assertEquals(initial, next);

            sequenceMapper.update(name, 110);
            next = sequenceMapper.next(name);
            Assertions.assertEquals(110, next);
        }
    }


}
