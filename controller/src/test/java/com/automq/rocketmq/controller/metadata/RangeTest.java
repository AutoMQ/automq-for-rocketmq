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

import com.automq.rocketmq.controller.metadata.database.dao.Range;
import com.automq.rocketmq.controller.metadata.database.mapper.RangeMapper;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer;

import java.io.IOException;
import java.util.List;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RangeTest extends DatabaseTestBase {

    @Test
    public void testRangeCRUD() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            RangeMapper rangeMapper = session.getMapper(RangeMapper.class);
            Range range = new Range();
            range.setRangeId(22);
            range.setStreamId(11L);
            range.setEpoch(1L);
            range.setStartOffset(1234L);
            range.setEndOffset(2345L);
            range.setBrokerId(33);

            int affectedRows = rangeMapper.create(range);
            Assertions.assertEquals(1, affectedRows);
            Assertions.assertTrue(range.getId() > 0);

            Range range1 = rangeMapper.getById(range.getId());
            Assertions.assertEquals(range, range1);

            // test getByRangeId
            range1 = rangeMapper.getByRangeId(range.getRangeId());
            Assertions.assertEquals(22, range1.getRangeId());
            Assertions.assertEquals(11, range1.getStreamId());
            Assertions.assertEquals(1, range1.getEpoch());
            Assertions.assertEquals(1234, range1.getStartOffset());
            Assertions.assertEquals(2345, range1.getEndOffset());
            Assertions.assertEquals(33, range1.getBrokerId());

            // test listByStreamId
            List<Range> ranges = rangeMapper.listByStreamId(range1.getStreamId());
            Assertions.assertNotNull(ranges);
            Assertions.assertEquals(1, ranges.size());
            Assertions.assertEquals(range, ranges.get(0));

            // test listByBrokerId
            List<Range> ranges1 = rangeMapper.listByBrokerId(range1.getBrokerId());
            Assertions.assertNotNull(ranges1);
            Assertions.assertEquals(1, ranges.size());

            // test get
            Range range2 = rangeMapper.get(range.getRangeId(), null, range.getBrokerId());
            Assertions.assertEquals(range, range2);

            ranges = rangeMapper.list(null, range.getStreamId(), 2000L);
            Assertions.assertEquals(1, ranges.size());
            Assertions.assertEquals(range, ranges.get(0));

            // test delete
            rangeMapper.delete(range2.getRangeId(), null);
            List<Range> ranges2 = rangeMapper.list(null, null, null);
            Assertions.assertTrue(ranges2.isEmpty());
        }
    }
}
