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

package com.automq.rocketmq.proxy.remoting.activity;

import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.junit.jupiter.api.Test;

import static com.automq.rocketmq.proxy.remoting.activity.CommonRemotingBehavior.BROKER_NAME_FIELD;
import static com.automq.rocketmq.proxy.remoting.activity.CommonRemotingBehavior.BROKER_NAME_FIELD_FOR_SEND_MESSAGE_V2;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

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
        assertEquals(commonRemotingBehavior.checkVersion(request).getCode(), ResponseCode.VERSION_NOT_SUPPORTED);

        request.addExtField(BROKER_NAME_FIELD, "bname");
        assertEquals(commonRemotingBehavior.checkVersion(request).getCode(), ResponseCode.VERSION_NOT_SUPPORTED);

        request.addExtField(BROKER_NAME_FIELD_FOR_SEND_MESSAGE_V2, "bname");
        assertNull(commonRemotingBehavior.checkVersion(request));

        request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);
        request.addExtField(BROKER_NAME_FIELD_FOR_SEND_MESSAGE_V2, "bname");
        assertEquals(commonRemotingBehavior.checkVersion(request).getCode(), ResponseCode.VERSION_NOT_SUPPORTED);

        request.addExtField(BROKER_NAME_FIELD, "bname");
        assertNull(commonRemotingBehavior.checkVersion(request));
    }
}