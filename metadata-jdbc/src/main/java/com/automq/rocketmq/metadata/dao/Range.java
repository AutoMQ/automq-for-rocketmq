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

import java.util.Objects;

public class Range {

    Long id;

    Integer rangeId;

    Long streamId;

    Long epoch;

    Long startOffset;

    Long endOffset;

    Integer nodeId;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Integer getRangeId() {
        return rangeId;
    }

    public void setRangeId(Integer rangeId) {
        this.rangeId = rangeId;
    }

    public Long getStreamId() {
        return streamId;
    }

    public void setStreamId(Long streamId) {
        this.streamId = streamId;
    }

    public Long getEpoch() {
        return epoch;
    }

    public void setEpoch(Long epoch) {
        this.epoch = epoch;
    }

    public Long getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(Long startOffset) {
        this.startOffset = startOffset;
    }

    public Long getEndOffset() {
        return endOffset;
    }

    public void setEndOffset(Long endOffset) {
        this.endOffset = endOffset;
    }

    public Integer getNodeId() {
        return nodeId;
    }

    public void setNodeId(Integer nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Range range = (Range) o;
        return Objects.equals(id, range.id) && Objects.equals(rangeId, range.rangeId) && Objects.equals(streamId, range.streamId) && Objects.equals(epoch, range.epoch) && Objects.equals(startOffset, range.startOffset) && Objects.equals(endOffset, range.endOffset) && Objects.equals(nodeId, range.nodeId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, rangeId, streamId, epoch, startOffset, endOffset, nodeId);
    }
}
