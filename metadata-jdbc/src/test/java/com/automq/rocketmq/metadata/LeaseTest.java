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

import com.automq.rocketmq.metadata.mapper.LeaseMapper;
import com.automq.rocketmq.metadata.dao.Lease;
import java.io.IOException;
import java.util.Calendar;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class LeaseTest extends DatabaseTestBase {

    @BeforeEach
    public void setUp() throws IOException {
        try (SqlSession session = getSessionFactory().openSession()) {
            LeaseMapper mapper = session.getMapper(LeaseMapper.class);
            Lease lease = new Lease();
            lease.setNodeId(0);
            lease.setEpoch(0);
            Calendar calendar = Calendar.getInstance();
            calendar.set(2023, Calendar.JANUARY, 1);
            lease.setExpirationTime(calendar.getTime());
            mapper.update(lease);
            session.commit();
        }
    }

    @Test
    @Order(1)
    public void testInitLease() throws IOException {
        try (SqlSession session = getSessionFactory().openSession()) {
            LeaseMapper leaseMapper = session.getMapper(LeaseMapper.class);
            Lease lease = leaseMapper.current();
            Assertions.assertTrue(lease.expired());
        }
    }

    @Test
    @Order(2)
    public void testLeaderElection() throws IOException {
        try (SqlSession session = getSessionFactory().openSession(false)) {
            LeaseMapper leaseMapper = session.getMapper(LeaseMapper.class);
            Lease lease = leaseMapper.current();
            if (lease.expired()) {
                Lease inspect = leaseMapper.currentWithWriteLock();
                if (lease.getEpoch() == inspect.getEpoch() && lease.getNodeId() == inspect.getNodeId()) {
                    inspect.setEpoch(inspect.getEpoch() + 1);
                    inspect.setNodeId(1);
                    Calendar calendar = Calendar.getInstance();
                    calendar.add(Calendar.SECOND, 30);
                    inspect.setExpirationTime(calendar.getTime());
                    int affectedRows = leaseMapper.update(inspect);
                    Assertions.assertEquals(1, affectedRows);
                    session.commit();
                } else {
                    Assertions.fail("Should not reach here");
                }
            }

            lease = leaseMapper.current();
            Assertions.assertFalse(lease.expired());
            Assertions.assertEquals(1, lease.getNodeId());
            Assertions.assertEquals(1, lease.getEpoch());
        }
    }
}
