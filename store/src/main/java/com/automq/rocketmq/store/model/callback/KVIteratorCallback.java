package com.automq.rocketmq.store.model.callback;

/**
 * Callback to execute once a key-value pair is read from KV store.
 */
public interface KVIteratorCallback {
    /**
     * Callback to execute once a key-value pair is read from KV store.
     *
     * @param key   config entry name
     * @param value config value, normally in format of JSON.
     */
    void onRead(byte[] key, byte[] value);
}
