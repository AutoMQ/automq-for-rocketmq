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

package com.automq.rocketmq.metadata.dao;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class S3ObjectCriteria {
    Long streamId;

    List<Long> ids;

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

        public S3ObjectCriteriaBuilder addAll(Collection<Long> ids) {
            if (null == criteria.ids) {
                criteria.ids = new ArrayList<>();
            }
            criteria.ids.addAll(ids);
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
