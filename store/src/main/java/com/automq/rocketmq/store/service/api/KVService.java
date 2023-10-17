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

package com.automq.rocketmq.store.service.api;

import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.model.kv.BatchRequest;
import com.automq.rocketmq.store.model.kv.IteratorCallback;
import com.automq.rocketmq.store.model.kv.KVReadOptions;

public interface KVService {
    /**
     * Get value with specified key from backend kv engine.
     *
     * @param namespace the namespace storing required the kv pair
     * @param key the key for querying
     * @param readOptions the read options
     * @return the value of the specified key
     * @throws StoreException if backend engine fails
     */
    byte[] get(final String namespace, final byte[] key, KVReadOptions readOptions) throws StoreException;

    default byte[] get(final String namespace, final byte[] key) throws StoreException {
        return get(namespace, key, new KVReadOptions());
    }

    /**
     * Iterate all the k-v pairs.
     *
     * @param namespace the namespace storing required the k-v pair
     * @param callback the iterator will call {@link IteratorCallback#onRead} to consume the kv pair
     * @param readOptions the read options
     * @throws StoreException if backend engine fails
     */
    void iterate(final String namespace, IteratorCallback callback, KVReadOptions readOptions) throws StoreException;

    default void iterate(final String namespace, IteratorCallback callback) throws StoreException {
        iterate(namespace, callback, new KVReadOptions());
    }

    /**
     * Iterate the k-v pair with the given prefix, start and end.
     *
     * <p>The user can either use {@code prefix}, {@code prefix} with {@code start},
     * {@code prefix} with {@code end}, or {@code start} with {@code end}.
     * When both the prefix and start are specified, parameter {@code start} has higher priority.
     *
     * @param namespace the namespace storing required the k-v pair
     * @param prefix iterate the kv pair with the specified prefix
     * @param start the lower bound to start iterate
     * @param end the upper bound to end iterate
     * @param callback the iterator will call {@link IteratorCallback#onRead} to consume the kv pair
     * @throws StoreException if backend engine fails
     */
    void iterate(final String namespace, final byte[] prefix, final byte[] start,
        final byte[] end, IteratorCallback callback, KVReadOptions readOptions) throws StoreException;

    default void iterate(final String namespace, final byte[] prefix, final byte[] start,
        final byte[] end, IteratorCallback callback) throws StoreException {
        iterate(namespace, prefix, start, end, callback, new KVReadOptions());
    }

    /**
     * Put the kv pair into the backend engine.
     *
     * @param namespace the namespace storing required the k-v pair
     * @param key the key for inserting
     * @param value the value for inserting
     * @throws StoreException if backend engine fails
     */
    void put(final String namespace, byte[] key, byte[] value) throws StoreException;

    /**
     * Put or delete the kv pair in batch.
     *
     * @param requests the mutation requests
     * @throws StoreException if backend engine fails
     */
    void batch(BatchRequest... requests) throws StoreException;

    /**
     * Delete value with specified key from backend kv engine.
     *
     * @param namespace the namespace storing required the k-v pair
     * @param key the key for deleting
     * @throws StoreException if backend engine fails
     */
    void delete(final String namespace, byte[] key) throws StoreException;

    /**
     * Forced dirty pages to the hard disk.
     *
     * @param sync synchronous or not
     * @throws StoreException if backend engine fails
     */
    void flush(boolean sync) throws StoreException;

    /**
     * Open the backend engine.
     *
     * @throws StoreException if backend engine fails
     */
    void open() throws StoreException;

    /**
     * Flush all dirty pages and shutdown the backend engine.
     *
     * @throws StoreException if backend engine fails
     */
    void close() throws StoreException;

    /**
     * Close and delete all data in the backend engine.
     *
     * @throws StoreException if backend engine fails
     */
    void destroy() throws StoreException;

    /**
     * Take a snapshot of the current kv store.
     * @return the snapshot version id
     * @throws StoreException
     */
    long takeSnapshot() throws StoreException;

    /**
     * Release the snapshot with the given snapshot id.
     * @param snapshotVersionId the snapshot version id
     * @throws StoreException
     */
    void releaseSnapshot(long snapshotVersionId) throws StoreException;
}
