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

import com.automq.rocketmq.controller.metadata.database.mapper.LeaseMapper;
import com.automq.rocketmq.controller.metadata.database.dao.Lease;
import java.io.IOException;
import java.util.Calendar;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class LeaseTest extends DatabaseTestBase {

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
