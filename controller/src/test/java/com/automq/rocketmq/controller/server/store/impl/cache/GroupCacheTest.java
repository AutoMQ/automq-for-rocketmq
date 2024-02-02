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

package com.automq.rocketmq.controller.server.store.impl.cache;

import apache.rocketmq.controller.v1.GroupStatus;
import apache.rocketmq.controller.v1.GroupType;
import apache.rocketmq.controller.v1.SubscriptionMode;
import com.automq.rocketmq.metadata.dao.Group;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class GroupCacheTest {

    @Test
    public void testCache() {
        GroupCache cache = new GroupCache();
        Group group = new Group();
        group.setGroupType(GroupType.GROUP_TYPE_STANDARD);
        group.setStatus(GroupStatus.GROUP_STATUS_DELETED);
        group.setId(1L);
        group.setName("G1");
        group.setSubMode(SubscriptionMode.SUB_MODE_POP);
        group.setDeadLetterTopicId(2L);
        group.setMaxDeliveryAttempt(3);

        cache.apply(List.of(group));

        Assertions.assertEquals(0, cache.groupQuantity());
    }

}