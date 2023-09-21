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

package com.automq.rocketmq.controller.metadata.database.tasks;

import com.automq.rocketmq.controller.metadata.database.DefaultMetadataStore;
import com.automq.rocketmq.controller.metadata.database.dao.Node;
import com.automq.rocketmq.controller.metadata.database.mapper.NodeMapper;
import java.util.List;
import org.apache.ibatis.session.SqlSession;

public class ScanNodeTask extends ScanTask {

    public ScanNodeTask(DefaultMetadataStore metadataStore) {
        super(metadataStore);
    }

    @Override
    public void run() {
        LOGGER.info("ScanNodeTask starts");
        try {
            try (SqlSession session = this.metadataStore.getSessionFactory().openSession()) {
                NodeMapper mapper = session.getMapper(NodeMapper.class);
                List<Node> nodes = mapper.list(this.lastScanTime);
                updateBrokers(nodes);
            }
        } catch (Throwable e) {
            LOGGER.error("Failed to scan nodes from database", e);
        }
        LOGGER.info("ScanNodeTask completed");
    }

    private void updateBrokers(List<Node> nodes) {
        if (null != nodes) {
            for (Node node : nodes) {
                this.metadataStore.addBroker(node);
                if (null == this.lastScanTime) {
                    this.lastScanTime = node.getUpdateTime();
                } else if (node.getUpdateTime().after(this.lastScanTime)) {
                    this.lastScanTime = node.getUpdateTime();
                }
            }
        }
    }
}
