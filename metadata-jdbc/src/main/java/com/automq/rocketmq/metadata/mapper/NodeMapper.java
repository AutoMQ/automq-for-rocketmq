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

package com.automq.rocketmq.metadata.mapper;

import com.automq.rocketmq.metadata.dao.Node;
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

    Node get(
        @Param("id") Integer id,
        @Param("name") String name,
        @Param("instanceId") String instanceId,
        @Param("volumeId") String volumeId
    );

    void update(Node node);

    List<Node> list(@Param("updateTime") Date updateTime);

    void delete(Integer id);
}
