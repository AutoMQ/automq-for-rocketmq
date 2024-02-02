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

package com.automq.rocketmq.controller.server.store.impl.cache;

import com.automq.rocketmq.metadata.dao.Group;
import com.google.common.base.Strings;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GroupCache {

    private static final Logger LOGGER = LoggerFactory.getLogger(GroupCache.class);

    private final ConcurrentMap<String, Long> naming;

    private final ConcurrentMap<Long, Group> groups;

    public GroupCache() {
        naming = new ConcurrentHashMap<>();
        groups = new ConcurrentHashMap<>();
    }

    public Group byName(String name) {
        if (Strings.isNullOrEmpty(name)) {
            return null;
        }
        Long groupId = naming.get(name);
        return byId(groupId);
    }

    public Group byId(Long groupId) {
        if (null == groupId) {
            return null;
        }

        return groups.get(groupId);
    }

    public void apply(List<Group> groups) {
        if (null == groups || groups.isEmpty()) {
            return;
        }

        for (Group group : groups) {
            refresh(group);
        }
    }

    private void refresh(Group group) {
        LOGGER.info("Refresh cache for group[id={}, name={}, status={}]", group.getId(), group.getName(),
            group.getStatus());
        switch (group.getStatus()) {
            case GROUP_STATUS_DELETED -> {
                naming.remove(group.getName());
                groups.remove(group.getId());
            }
            case GROUP_STATUS_ACTIVE -> {
                naming.put(group.getName(), group.getId());
                groups.put(group.getId(), group);
            }
        }
    }

    public int groupQuantity() {
        return groups.size();
    }

}
