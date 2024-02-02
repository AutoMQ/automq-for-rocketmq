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

package com.automq.rocketmq.proxy.remoting.activity;

import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.junit.jupiter.api.Test;

import static com.automq.rocketmq.proxy.remoting.activity.CommonRemotingBehavior.BROKER_NAME_FIELD;
import static com.automq.rocketmq.proxy.remoting.activity.CommonRemotingBehavior.BROKER_NAME_FIELD_FOR_SEND_MESSAGE_V2;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CommonRemotingBehaviorTest {
    private final CommonRemotingBehavior commonRemotingBehavior = new CommonRemotingBehavior() {
    };

    @Test
    void dstBrokerName() {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE_V2, null);
        request.addExtField("PLACE_HOLDER_KEY", "PLACE_HOLDER_VALUE");

        assertNull(commonRemotingBehavior.dstBrokerName(request));
        String brokerName = "bname";

        request.addExtField(BROKER_NAME_FIELD, brokerName);
        assertNull(commonRemotingBehavior.dstBrokerName(request));

        request.addExtField(BROKER_NAME_FIELD_FOR_SEND_MESSAGE_V2, brokerName);
        assertEquals(brokerName, commonRemotingBehavior.dstBrokerName(request));

        request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);
        request.addExtField(BROKER_NAME_FIELD_FOR_SEND_MESSAGE_V2, brokerName);
        assertNull(commonRemotingBehavior.dstBrokerName(request));
        request.addExtField(BROKER_NAME_FIELD, brokerName);
        assertEquals(brokerName, commonRemotingBehavior.dstBrokerName(request));
    }

    @Test
    void checkVersion() {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE_V2, null);
        request.addExtField("PLACE_HOLDER_KEY", "PLACE_HOLDER_VALUE");
        assertTrue(commonRemotingBehavior.checkRequiredField(request).isPresent());
        assertEquals(commonRemotingBehavior.checkRequiredField(request).get().getCode(), ResponseCode.VERSION_NOT_SUPPORTED);

        request.addExtField(BROKER_NAME_FIELD, "bname");
        assertTrue(commonRemotingBehavior.checkRequiredField(request).isPresent());
        assertEquals(commonRemotingBehavior.checkRequiredField(request).get().getCode(), ResponseCode.VERSION_NOT_SUPPORTED);

        request.addExtField(BROKER_NAME_FIELD_FOR_SEND_MESSAGE_V2, "bname");
        assertTrue(commonRemotingBehavior.checkRequiredField(request).isEmpty());

        request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);
        request.addExtField(BROKER_NAME_FIELD_FOR_SEND_MESSAGE_V2, "bname");
        assertTrue(commonRemotingBehavior.checkRequiredField(request).isPresent());
        assertEquals(commonRemotingBehavior.checkRequiredField(request).get().getCode(), ResponseCode.VERSION_NOT_SUPPORTED);

        request.addExtField(BROKER_NAME_FIELD, "bname");
        assertTrue(commonRemotingBehavior.checkRequiredField(request).isEmpty());
    }
}