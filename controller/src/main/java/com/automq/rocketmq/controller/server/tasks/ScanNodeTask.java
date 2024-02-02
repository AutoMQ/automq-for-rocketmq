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

package com.automq.rocketmq.controller.server.tasks;

import com.automq.rocketmq.common.exception.ControllerException;
import com.automq.rocketmq.controller.MetadataStore;
import com.automq.rocketmq.metadata.dao.Node;
import com.automq.rocketmq.metadata.mapper.NodeMapper;
import java.util.List;
import org.apache.ibatis.session.SqlSession;

public class ScanNodeTask extends ScanTask {

    public ScanNodeTask(MetadataStore metadataStore) {
        super(metadataStore);
    }

    @Override
    public void process() throws ControllerException {
        try (SqlSession session = this.metadataStore.openSession()) {
            NodeMapper mapper = session.getMapper(NodeMapper.class);
            List<Node> nodes = mapper.list(this.lastScanTime);
            if (!nodes.isEmpty() && LOGGER.isDebugEnabled()) {
                for (Node node : nodes) {
                    LOGGER.debug("Found broker node: {}", node);
                }
            }
            updateBrokers(nodes);
        }
    }

    private void updateBrokers(List<Node> nodes) {
        if (null != nodes) {
            for (Node node : nodes) {
                this.metadataStore.addBrokerNode(node);
                if (null == this.lastScanTime) {
                    this.lastScanTime = node.getUpdateTime();
                } else if (node.getUpdateTime().after(this.lastScanTime)) {
                    this.lastScanTime = node.getUpdateTime();
                }
            }
        }
    }
}
