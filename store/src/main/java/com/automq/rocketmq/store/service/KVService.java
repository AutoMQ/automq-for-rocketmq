package com.automq.rocketmq.store.service;

import com.automq.rocketmq.store.model.callback.KVIteratorCallback;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import org.rocksdb.RocksDBException;

public interface KVService {
    Charset CHARSET = StandardCharsets.UTF_8;

    // TODO: Map RocksDBException into KVStoreException

    /**
     * Get value with specified key from backend kv engine.
     *
     * @param partition the partition storing required the kv pair
     * @param key the key for querying
     * @return the value of the specified key
     * @throws RocksDBException if backend engine fails
     */
    byte[] get(final String partition, final byte[] key) throws RocksDBException;

    /**
     * Iterate all the k-v pairs.
     *
     * @param partition the partition storing required the k-v pair
     * @param callback the iterator will call {@link KVIteratorCallback#onRead} to consume the kv pair
     * @throws RocksDBException if backend engine fails
     */
    void iterate(final String partition, KVIteratorCallback callback) throws RocksDBException;

    /**
     * Iterate the k-v pair with the given prefix, start and end.
     *
     * <p>The user can either use {@code prefix}, {@code prefix} with {@code start},
     * {@code prefix} with {@code end}, or {@code start} with {@code end}.
     * When both the prefix and start are specified, parameter {@code start} has higher priority.
     *
     * @param partition the partition storing required the k-v pair
     * @param prefix iterate the kv pair with the specified prefix
     * @param start the lower bound to start iterate
     * @param end the upper bound to end iterate
     * @param callback the iterator will call {@link KVIteratorCallback#onRead} to consume the kv pair
     * @throws RocksDBException if backend engine fails
     */
    void iterate(final String partition, String prefix, final String start,
        final String end, KVIteratorCallback callback) throws RocksDBException;

    /**
     * Put the kv pair into the backend engine.
     *
     * @param partition the partition storing required the k-v pair
     * @param key the key for inserting
     * @param value the value for inserting
     * @throws RocksDBException if backend engine fails
     */
    void put(final String partition, byte[] key, byte[] value) throws RocksDBException;

    /**
     * Delete value with specified key from backend kv engine.
     *
     * @param partition the partition storing required the k-v pair
     * @param key the key for deleting
     * @throws RocksDBException if backend engine fails
     */
    void delete(final String partition, byte[] key) throws RocksDBException;

    /**
     * Forced dirty pages to the hard disk.
     *
     * @param sync synchronous or not
     * @throws RocksDBException if backend engine fails
     */
    void flush(boolean sync) throws RocksDBException;

    /**
     * Flush all dirty pages and shutdown the backend engine.
     */
    void close();

    /**
     * Delete all data in the backend engine.
     *
     * @throws RocksDBException if backend engine fails
     */
    void destroy() throws RocksDBException;
}
