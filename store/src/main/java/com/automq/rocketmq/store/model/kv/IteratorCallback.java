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

package com.automq.rocketmq.store.model.kv;

/**
 * Callback to execute once a key-value pair is read from KV store.
 */
public interface IteratorCallback {
    /**
     * Callback to execute once a key-value pair is read from KV store.
     *
     * @param key   config entry name
     * @param value config value, normally in format of JSON.
     */
    void onRead(byte[] key, byte[] value);
}
