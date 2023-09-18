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

package com.automq.rocketmq.store.model.message;

import com.automq.rocketmq.common.model.generated.Message;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public record TagFilter(String expression) implements Filter {
    @Override
    public FilterType type() {
        return FilterType.TAG;
    }

    @Override
    public List<Message> apply(List<Message> messageList) {
        return messageList.stream()
            .filter(message -> Objects.equals(message.tag(), expression))
            .collect(Collectors.toList());
    }
}
