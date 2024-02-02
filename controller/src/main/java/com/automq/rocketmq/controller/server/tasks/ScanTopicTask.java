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
import com.automq.rocketmq.metadata.dao.Topic;
import com.automq.rocketmq.metadata.mapper.TopicMapper;
import java.util.List;
import org.apache.ibatis.session.SqlSession;

public class ScanTopicTask extends ScanTask {

    public ScanTopicTask(MetadataStore metadataStore) {
        super(metadataStore);
    }

    @Override
    public void process() throws ControllerException {
        try (SqlSession session = metadataStore.openSession()) {
            TopicMapper mapper = session.getMapper(TopicMapper.class);
            List<Topic> topics = mapper.list(null, lastScanTime);

            metadataStore.applyTopicChange(topics);

            // Update last scan time
            if (null != topics && !topics.isEmpty()) {
                for (Topic topic: topics) {
                    if (null == lastScanTime || topic.getUpdateTime().after(lastScanTime)) {
                        lastScanTime = topic.getUpdateTime();
                    }
                }
            }
        }

    }
}
