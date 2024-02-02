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

import com.automq.rocketmq.metadata.dao.Node;
import com.automq.rocketmq.metadata.mapper.NodeMapper;
import java.io.IOException;
import java.util.List;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class NodeTest extends DatabaseTestBase {

    @Test
    @Order(1)
    public void testListNodes() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            NodeMapper nodeMapper = session.getMapper(NodeMapper.class);
            List<Node> list = nodeMapper.list(null);
            Assertions.assertTrue(list.isEmpty());
        }
    }

    @Test
    @Order(2)
    public void testNode_CRUD() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            NodeMapper nodeMapper = session.getMapper(NodeMapper.class);
            Node node = new Node();

            String name = "Test-1";
            String address = "localhost:1234";
            String instanceId = "i-asdf";
            String volumeId = "v-1234";
            String hostName = "localhost";
            String vpcId = "vpc-1234";

            node.setName(name);
            node.setAddress(address);
            node.setInstanceId(instanceId);
            node.setVolumeId(volumeId);
            node.setHostName(hostName);
            node.setVpcId(vpcId);

            int affectedRows = nodeMapper.create(node);

            Assertions.assertEquals(1, affectedRows);

            List<Node> nodes = nodeMapper.list(null);
            Assertions.assertEquals(1, nodes.size());
            Node node1 = nodes.get(0);
            Assertions.assertEquals(name, node1.getName());
            Assertions.assertEquals(instanceId, node1.getInstanceId());
            Assertions.assertEquals(address, node1.getAddress());
            Assertions.assertEquals(volumeId, node1.getVolumeId());
            Assertions.assertEquals(hostName, node1.getHostName());
            Assertions.assertEquals(vpcId, node1.getVpcId());
            Assertions.assertEquals(0, node1.getEpoch());

            nodeMapper.delete(node.getId());

            nodes = nodeMapper.list(null);
            Assertions.assertTrue(nodes.isEmpty());
        }
    }

    @Test
    @Order(3)
    public void testUpdate() throws IOException {
        try (SqlSession session = this.getSessionFactory().openSession()) {
            NodeMapper nodeMapper = session.getMapper(NodeMapper.class);
            Node node = new Node();
            node.setName("Test-1");
            node.setAddress("localhost:1234");
            node.setInstanceId("i-asdf");
            int affectedRows = nodeMapper.create(node);
            Assertions.assertEquals(1, affectedRows);

            Node node1 = nodeMapper.get(null, null, node.getInstanceId(), null);
            Assertions.assertEquals(0, node1.getEpoch());
            Assertions.assertEquals(node.getId(), node1.getId());
            node.setEpoch(node.getEpoch() + 1);
            node.setAddress("localhost:2345");
            nodeMapper.update(node);
            node1 = nodeMapper.get(null, null, node1.getInstanceId(), null);
            Assertions.assertEquals(1, node1.getEpoch());
            Assertions.assertEquals("localhost:2345", node1.getAddress());
        }
    }

}
