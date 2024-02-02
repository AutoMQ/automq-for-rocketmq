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

package com.automq.rocketmq.metadata.dao;

import apache.rocketmq.controller.v1.S3ObjectState;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

public class S3ObjectCriteria {
    Long streamId;

    List<Long> ids;

    S3ObjectState state;

    Date expiredTimestamp;

    public static class S3ObjectCriteriaBuilder {
        S3ObjectCriteriaBuilder() {

        }

        private final S3ObjectCriteria criteria = new S3ObjectCriteria();

        public S3ObjectCriteria build() {
            return criteria;
        }

        public S3ObjectCriteriaBuilder withStreamId(long streamId) {
            criteria.streamId = streamId;
            return this;
        }

        public S3ObjectCriteriaBuilder addObjectIds(Collection<Long> ids) {
            if (null == criteria.ids) {
                criteria.ids = new ArrayList<>();
            }
            criteria.ids.addAll(ids);
            return this;
        }

        public S3ObjectCriteriaBuilder withState(S3ObjectState state) {
            criteria.state = state;
            return this;
        }

        public S3ObjectCriteriaBuilder withExpiredTimestamp(Date expiredTimestamp) {
            criteria.expiredTimestamp = expiredTimestamp;
            return this;
        }
    }

    public static S3ObjectCriteriaBuilder newBuilder() {
        return new S3ObjectCriteriaBuilder();
    }

    public Long getStreamId() {
        return streamId;
    }

    public List<Long> getIds() {
        return ids;
    }
}
