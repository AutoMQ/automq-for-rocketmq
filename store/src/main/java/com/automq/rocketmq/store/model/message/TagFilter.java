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

import com.automq.rocketmq.common.model.FlatMessageExt;
import com.google.common.base.Strings;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class TagFilter implements Filter {
    public final static String SUB_ALL = "*";

    String expression;
    Set<String> tagSet;

    public TagFilter(String expression) {
        if (Strings.isNullOrEmpty(expression)) {
            throw new IllegalArgumentException("Expression can not be null or empty");
        }

        if (expression.contains(SUB_ALL)) {
            throw new IllegalArgumentException("Expression of TagFilter can not contain *, use Filter#DEFAULT_FILTER instead");
        }

        this.expression = expression;
        String[] tags = expression.split("\\|\\|");
        if (tags.length > 0) {
            this.tagSet = Arrays.stream(tags)
                .map(String::trim)
                // Filter out blank tags and SUB_ALL
                .filter(tag -> !tag.isBlank())
                .collect(Collectors.toSet());
        } else {
            throw new IllegalArgumentException("Split expression failed");
        }
    }

    @Override
    public FilterType type() {
        return FilterType.TAG;
    }

    @Override
    public String expression() {
        return expression;
    }

    @Override
    public List<FlatMessageExt> doFilter(List<FlatMessageExt> messageList) {
        return messageList.stream()
            .filter(messageExt -> !Strings.isNullOrEmpty(messageExt.message().tag()) && tagSet.contains(messageExt.message().tag()))
            .collect(Collectors.toList());
    }

    @Override
    public boolean doFilter(String tag) {
        return tagSet.contains(tag);
    }

    @Override
    public String toString() {
        return "TagFilter{" +
            "expression='" + expression + '\'' +
            '}';
    }
}
