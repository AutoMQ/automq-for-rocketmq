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

import apache.rocketmq.controller.v1.Range;
import com.automq.rocketmq.controller.metadata.database.mapper.RangeMapper;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer;

import java.io.IOException;
import java.util.List;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RangeTest extends DatabaseTestBase {

    @Test
    @Order(1)
    public void testCreateRange() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);
            Range range = Range.newBuilder().
                setRangeId(22).setStreamId(11).
                setEpoch(1).setStartOffset(1234).
                setEndOffset(2345).build();

            int affectedRows = rangeMapper.create(range);
            Assertions.assertEquals(1, affectedRows);

            Range range1 = rangeMapper.getByRangeId(range.getRangeId());
            Assertions.assertEquals(22, range1.getRangeId());
            Assertions.assertEquals(11, range1.getStreamId());
            Assertions.assertEquals(1, range1.getEpoch());
            Assertions.assertEquals(1234, range1.getStartOffset());
            Assertions.assertEquals(2345, range1.getEndOffset());

            rangeMapper.delete(range1.getRangeId());
            List<Range> ranges = rangeMapper.list();
            Assertions.assertTrue(ranges.isEmpty());
        }
    }

    @Test
    @Order(2)
    public void testListByStreamId() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);
            Range range = Range.newBuilder().
                setRangeId(1).setStreamId(11).
                setEpoch(1).setStartOffset(1234).
                setEndOffset(2345).build();

            int affectedRows = rangeMapper.create(range);
            Assertions.assertEquals(1, affectedRows);

            Range range1 = rangeMapper.getByRangeId(range.getRangeId());
            List<Range> ranges = rangeMapper.listByStreamId(range1.getStreamId());
            Assertions.assertNotNull(ranges);
            Assertions.assertEquals(1, ranges.size());
            rangeMapper.delete(range1.getRangeId());
        }
    }


    @Test
    @Order(3)
    public void testListByBrokerId() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);
            Range range = Range.newBuilder().
                setRangeId(1).setStreamId(11).
                setEpoch(1).setStartOffset(1234).
                setEndOffset(2345).build();

            int affectedRows = rangeMapper.create(range);
            Assertions.assertEquals(1, affectedRows);

            Range range1 = rangeMapper.getByRangeId(range.getRangeId());
            List<Range> ranges = rangeMapper.listByBrokerId(range1.getBrokerId());
            Assertions.assertNotNull(ranges);
            Assertions.assertEquals(1, ranges.size());
            rangeMapper.delete(range1.getRangeId());
        }
    }
}
