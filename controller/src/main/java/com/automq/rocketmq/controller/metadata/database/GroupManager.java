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

package com.automq.rocketmq.controller.metadata.database;

import apache.rocketmq.controller.v1.Code;
import apache.rocketmq.controller.v1.ConsumerGroup;
import apache.rocketmq.controller.v1.CreateGroupRequest;
import apache.rocketmq.controller.v1.GroupStatus;
import apache.rocketmq.controller.v1.GroupType;
import apache.rocketmq.controller.v1.TopicStatus;
import apache.rocketmq.controller.v1.UpdateGroupRequest;
import com.automq.rocketmq.controller.exception.ControllerException;
import com.automq.rocketmq.controller.metadata.MetadataStore;
import com.automq.rocketmq.controller.metadata.database.cache.GroupCache;
import com.automq.rocketmq.controller.metadata.database.cache.Inflight;
import com.automq.rocketmq.controller.metadata.database.dao.Group;
import com.automq.rocketmq.controller.metadata.database.dao.GroupCriteria;
import com.automq.rocketmq.controller.metadata.database.dao.Topic;
import com.automq.rocketmq.controller.metadata.database.mapper.GroupMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.TopicMapper;
import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nonnull;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GroupManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(GroupMapper.class);

    final GroupCache groupCache;

    final ConcurrentMap<Long, Inflight<ConsumerGroup>> idRequests;
    final ConcurrentMap<String, Inflight<ConsumerGroup>> nameRequests;

    private final MetadataStore metadataStore;

    public GroupManager(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
        this.groupCache = new GroupCache();
        this.idRequests = new ConcurrentHashMap<>();
        this.nameRequests = new ConcurrentHashMap<>();
    }

    public CompletableFuture<Long> createGroup(String groupName, int maxRetry, GroupType type, long deadLetterTopicId) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        for (; ; ) {
            if (metadataStore.isLeader()) {
                try (SqlSession session = metadataStore.openSession()) {
                    if (!metadataStore.maintainLeadershipWithSharedLock(session)) {
                        continue;
                    }
                    GroupMapper groupMapper = session.getMapper(GroupMapper.class);
                    List<Group> groups = groupMapper.byCriteria(GroupCriteria.newBuilder().setGroupName(groupName).build());
                    if (!groups.isEmpty()) {
                        ControllerException e = new ControllerException(Code.DUPLICATED_VALUE, String.format("Group name '%s' is not available", groupName));
                        future.completeExceptionally(e);
                        return future;
                    }

                    if (deadLetterTopicId > 0) {
                        TopicMapper topicMapper = session.getMapper(TopicMapper.class);
                        Topic t = topicMapper.get(deadLetterTopicId, null);
                        if (null == t || t.getStatus() == TopicStatus.TOPIC_STATUS_DELETED) {
                            String msg = String.format("Specified dead letter topic[topic-id=%d] does not exist",
                                deadLetterTopicId);
                            ControllerException e = new ControllerException(Code.NOT_FOUND_VALUE, msg);
                            future.completeExceptionally(e);
                            return future;
                        }
                    }

                    Group group = new Group();
                    group.setName(groupName);
                    group.setMaxDeliveryAttempt(maxRetry);
                    group.setDeadLetterTopicId(deadLetterTopicId);
                    group.setStatus(GroupStatus.GROUP_STATUS_ACTIVE);
                    group.setGroupType(type);
                    groupMapper.create(group);
                    session.commit();
                    // Cache group metadata
                    groupCache.apply(List.of(group));
                    future.complete(group.getId());
                }
            } else {
                CreateGroupRequest request = CreateGroupRequest.newBuilder()
                    .setName(groupName)
                    .setMaxRetryAttempt(maxRetry)
                    .setGroupType(type)
                    .setDeadLetterTopicId(deadLetterTopicId)
                    .build();

                try {
                    metadataStore.controllerClient().createGroup(metadataStore.leaderAddress(), request).whenComplete((reply, e) -> {
                        if (null != e) {
                            future.completeExceptionally(e);
                        } else {
                            future.complete(reply.getGroupId());
                        }
                    });
                } catch (ControllerException e) {
                    future.completeExceptionally(e);
                }
            }
            break;
        }
        return future;
    }

    private void completeDescription(@Nonnull ConsumerGroup group) {
        Inflight<ConsumerGroup> idInflight = idRequests.remove(group.getGroupId());
        if (null != idInflight) {
            idInflight.complete(group);
        }

        Inflight<ConsumerGroup> nameInflight = nameRequests.remove(group.getName());
        if (null != nameInflight) {
            nameInflight.complete(group);
        }
    }

    private void completeDescriptionExceptionally(Long groupId, String groupName, Throwable e) {
        if (null != groupId) {
            Inflight<ConsumerGroup> idInflight = idRequests.remove(groupId);
            if (null != idInflight) {
                idInflight.completeExceptionally(e);
            }
        }

        if (!Strings.isNullOrEmpty(groupName)) {
            Inflight<ConsumerGroup> nameInflight = nameRequests.remove(groupName);
            if (null != nameInflight) {
                nameInflight.completeExceptionally(e);
            }
        }
    }

    public CompletableFuture<ConsumerGroup> describeGroup(Long groupId, String groupName) {
        Group cachedGroup = null;
        if (null != groupId) {
            cachedGroup = groupCache.byId(groupId);
        }

        if (null == cachedGroup && !Strings.isNullOrEmpty(groupName)) {
            cachedGroup = groupCache.byName(groupName);
        }

        if (null != cachedGroup) {
            return CompletableFuture.completedFuture(fromGroup(cachedGroup));
        }

        boolean queryNow = false;
        CompletableFuture<ConsumerGroup> future = new CompletableFuture<>();

        if (null != groupId) {
            switch (Helper.addFuture(groupId, future, idRequests)) {
                case COMPLETED -> {
                    return describeGroup(groupId, groupName);
                }
                case LEADER -> queryNow = true;
            }
        }

        if (!Strings.isNullOrEmpty(groupName)) {
            switch (Helper.addFuture(groupName, future, nameRequests)) {
                case COMPLETED -> {
                    return describeGroup(groupId, groupName);
                }
                case LEADER -> queryNow = true;
            }
        }

        if (queryNow) {
            metadataStore.asyncExecutor().submit(() -> {
                try (SqlSession session = metadataStore.openSession()) {
                    GroupMapper groupMapper = session.getMapper(GroupMapper.class);
                    List<Group> groups = groupMapper.byCriteria(GroupCriteria.newBuilder()
                        .setGroupId(groupId)
                        .setGroupName(groupName)
                        .build());
                    if (groups.isEmpty()) {
                        ControllerException e = new ControllerException(Code.NOT_FOUND_VALUE,
                            String.format("Group with group-id=%d is not found", groupId));
                        completeDescriptionExceptionally(groupId, groupName, e);
                    } else {
                        Group group = groups.get(0);

                        // Update cache
                        groupCache.apply(List.of(group));

                        // Complete futures
                        ConsumerGroup consumerGroup = fromGroup(group);
                        completeDescription(consumerGroup);
                    }
                }
            });
        }

        return future;
    }

    public CompletableFuture<ConsumerGroup> deleteGroup(long groupId) {
        return CompletableFuture.supplyAsync(() -> {
            try (SqlSession session = metadataStore.openSession()) {
                GroupMapper mapper = session.getMapper(GroupMapper.class);
                List<Group> groups = mapper.byCriteria(GroupCriteria.newBuilder().setGroupId(groupId).build());
                if (groups.isEmpty()) {
                    String message = String.format("Group[group-id=%d] is not found", groupId);
                    LOGGER.warn("Try to delete non-existing group[id={}]", groupId);
                    throw new CompletionException(new ControllerException(Code.NOT_FOUND_VALUE, message));
                }

                Group group = groups.get(0);
                if (GroupStatus.GROUP_STATUS_DELETED == group.getStatus()) {
                    LOGGER.warn("Group[id={}] has already been deleted", groupId);
                    String message = String.format("Group[group-id=%d] has already been deleted", groupId);
                    throw new CompletionException(new ControllerException(Code.NOT_FOUND_VALUE, message));
                }

                group.setStatus(GroupStatus.GROUP_STATUS_DELETED);
                mapper.update(group);
                session.commit();
                return fromGroup(group);
            }
        }, metadataStore.asyncExecutor());

    }

    private ConsumerGroup fromGroup(Group group) {
        return ConsumerGroup.newBuilder()
            .setGroupId(group.getId())
            .setName(group.getName())
            .setGroupType(group.getGroupType())
            .setMaxDeliveryAttempt(group.getMaxDeliveryAttempt())
            .setDeadLetterTopicId(group.getDeadLetterTopicId())
            .build();
    }

    public CompletableFuture<Collection<ConsumerGroup>> listGroups() {
        return CompletableFuture.supplyAsync(() -> {
            List<ConsumerGroup> groups = new ArrayList<>();

            try (SqlSession session = metadataStore.openSession()) {
                GroupMapper mapper = session.getMapper(GroupMapper.class);
                List<Group> list = mapper.byCriteria(GroupCriteria.newBuilder()
                    .setStatus(GroupStatus.GROUP_STATUS_ACTIVE)
                    .build());
                for (Group item : list) {
                    groups.add(fromGroup(item));
                }
            }

            return groups;
        });
    }

    public CompletableFuture<Void> updateGroup(UpdateGroupRequest request) {
        return CompletableFuture.supplyAsync(() -> {
            for (; ; ) {
                if (metadataStore.isLeader()) {
                    try (SqlSession session = metadataStore.openSession()) {
                        if (!metadataStore.maintainLeadershipWithSharedLock(session)) {
                            continue;
                        }

                        GroupMapper mapper = session.getMapper(GroupMapper.class);
                        List<Group> groups = mapper.byCriteria(GroupCriteria.newBuilder()
                            .setGroupId(request.getGroupId())
                            .setStatus(GroupStatus.GROUP_STATUS_ACTIVE)
                            .build());
                        if (groups.isEmpty()) {
                            String msg = String.format("Group[group-id=%d] is not found", request.getGroupId());
                            throw new CompletionException(new ControllerException(Code.NOT_FOUND_VALUE, msg));
                        }

                        Group group = groups.get(0);
                        if (request.getGroupType() != GroupType.GROUP_TYPE_UNSPECIFIED) {
                            group.setGroupType(request.getGroupType());
                        }

                        if (!Strings.isNullOrEmpty(request.getName())) {
                            group.setName(request.getName());
                        }

                        if (request.getDeadLetterTopicId() > 0) {
                            group.setDeadLetterTopicId(request.getDeadLetterTopicId());
                        }

                        if (request.getMaxRetryAttempt() > 0) {
                            group.setMaxDeliveryAttempt(request.getMaxRetryAttempt());
                        }
                        mapper.update(group);
                        session.commit();
                        break;
                    }
                } else {
                    try {
                        metadataStore.controllerClient().updateGroup(metadataStore.leaderAddress(), request).join();
                        break;
                    } catch (ControllerException e) {
                        throw new CompletionException(e);
                    }
                }
            }

            return null;
        });
    }
}
