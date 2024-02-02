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
