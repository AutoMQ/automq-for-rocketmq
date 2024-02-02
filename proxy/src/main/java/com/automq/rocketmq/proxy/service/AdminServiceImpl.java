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

package com.automq.rocketmq.proxy.service;

import java.util.List;
import org.apache.rocketmq.proxy.service.admin.AdminService;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;

public class AdminServiceImpl implements AdminService {
    @Override
    public boolean topicExist(String topic) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean createTopicOnTopicBrokerIfNotExist(String createTopic, String sampleTopic, int wQueueNum,
        int rQueueNum, boolean examineTopic, int retryCheckCount) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean createTopicOnBroker(String topic, int wQueueNum, int rQueueNum, List<BrokerData> curBrokerDataList,
        List<BrokerData> sampleBrokerDataList, boolean examineTopic, int retryCheckCount) throws Exception {
        throw new UnsupportedOperationException();
    }
}
