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
import com.automq.rocketmq.metadata.dao.Stream;
import com.automq.rocketmq.metadata.dao.StreamCriteria;
import com.automq.rocketmq.metadata.mapper.StreamMapper;
import java.util.List;
import org.apache.ibatis.session.SqlSession;

public class ScanStreamTask extends ScanTask {
    public ScanStreamTask(MetadataStore metadataStore) {
        super(metadataStore);
    }

    @Override
    public void process() throws ControllerException {
        try (SqlSession session = metadataStore.openSession()) {
            StreamMapper mapper = session.getMapper(StreamMapper.class);
            StreamCriteria criteria = StreamCriteria.newBuilder()
                .withUpdateTime(lastScanTime)
                .build();
            List<Stream> streams = mapper.byCriteria(criteria);
            metadataStore.applyStreamChange(streams);

            for (Stream stream : streams) {
                if (null == lastScanTime || stream.getUpdateTime().after(lastScanTime)) {
                    lastScanTime = stream.getUpdateTime();
                }
            }
        }
    }
}
