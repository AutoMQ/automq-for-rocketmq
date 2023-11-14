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