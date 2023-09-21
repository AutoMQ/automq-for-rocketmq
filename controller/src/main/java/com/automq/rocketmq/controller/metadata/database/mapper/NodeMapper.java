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

package com.automq.rocketmq.controller.metadata.database.mapper;

import com.automq.rocketmq.controller.metadata.database.dao.Node;
import java.util.Date;
import java.util.List;
import org.apache.ibatis.annotations.Param;

public interface NodeMapper {
    /**
     * Create a new node record in database.
     *
     * @param node Broker instance to persist
     * @return Generated broker identity
     */
    int create(Node node);

    Node get(@Param("name") String name,
        @Param("instanceId") String instanceId,
        @Param("volumeId") String volumeId
    );

    /**
     * Increase term of the node for each registration.
     *
     * @param id ID of the broker whose term should be increased
     * @return Number of rows affected, expected to be 1.
     */
    int increaseEpoch(int id);

    List<Node> list(@Param("updateTime") Date updateTime);

    void delete(int id);
}
