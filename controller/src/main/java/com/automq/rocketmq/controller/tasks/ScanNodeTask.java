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

package com.automq.rocketmq.controller.tasks;

import com.automq.rocketmq.controller.exception.ControllerException;
import com.automq.rocketmq.controller.metadata.MetadataStore;
import com.automq.rocketmq.controller.metadata.database.dao.Node;
import com.automq.rocketmq.controller.metadata.database.mapper.NodeMapper;
import com.google.gson.Gson;
import java.util.List;
import org.apache.ibatis.session.SqlSession;

public class ScanNodeTask extends ScanTask {

    private final Gson gson;

    public ScanNodeTask(MetadataStore metadataStore) {
        super(metadataStore);
        gson = new Gson();
    }

    @Override
    public void process() throws ControllerException {
        try (SqlSession session = this.metadataStore.openSession()) {
            NodeMapper mapper = session.getMapper(NodeMapper.class);
            List<Node> nodes = mapper.list(this.lastScanTime);
            if (!nodes.isEmpty() && LOGGER.isDebugEnabled()) {
                for (Node node : nodes) {
                    LOGGER.debug("Found broker node: {}", gson.toJson(node));
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
