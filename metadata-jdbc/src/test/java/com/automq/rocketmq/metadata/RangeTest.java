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

import com.automq.rocketmq.metadata.dao.Range;
import com.automq.rocketmq.metadata.mapper.RangeMapper;
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
            range.setNodeId(33);

            int affectedRows = rangeMapper.create(range);
            Assertions.assertEquals(1, affectedRows);
            Assertions.assertTrue(range.getId() > 0);

            Range range1 = rangeMapper.getById(range.getId());
            Assertions.assertEquals(range, range1);


            // test listByStreamId
            List<Range> ranges = rangeMapper.listByStreamId(range1.getStreamId());
            Assertions.assertNotNull(ranges);
            Assertions.assertEquals(1, ranges.size());
            Assertions.assertEquals(range, ranges.get(0));

            // test listByBrokerId
            List<Range> ranges1 = rangeMapper.listByNodeId(range1.getNodeId());
            Assertions.assertNotNull(ranges1);
            Assertions.assertEquals(1, ranges.size());

            // test get
            Range range2 = rangeMapper.get(range.getRangeId(), null, range.getNodeId());
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
