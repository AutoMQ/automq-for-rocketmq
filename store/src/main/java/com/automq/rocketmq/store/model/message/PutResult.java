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

public class PutResult {
    private final Status status;
    private final long offset;
    private final String transactionId;

    public PutResult(Status status, long offset) {
        this.status = status;
        this.offset = offset;
        this.transactionId = "";
    }

    public PutResult(Status status, long offset, String transactionId) {
        this.status = status;
        this.offset = offset;
        this.transactionId = transactionId;
    }

    public Status status() {
        return status;
    }

    public long offset() {
        return offset;
    }

    public String transactionId() {
        return transactionId;
    }

    public enum Status {
        PUT_OK,
        PUT_DELAYED,
        PUT_TRANSACTION_PREPARED,
    }
}
