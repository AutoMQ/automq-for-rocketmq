/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.automq.rocketmq.store.service.impl;

import com.automq.rocketmq.store.model.kv.BatchRequest;
import com.automq.rocketmq.store.model.kv.IteratorCallback;
import com.automq.rocketmq.store.service.KVService;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

public class RocksDBKVService implements KVService {
    private final String path;
    private final ColumnFamilyOptions columnFamilyOptions;
    private final DBOptions dbOptions;
    private final ConcurrentMap<String, ColumnFamilyHandle> columnFamilyNameHandleMap;
    private final RocksDB rocksDB;
    private volatile boolean stopped;

    public RocksDBKVService(String path) throws RocksDBException {
        this.path = path;
        this.columnFamilyOptions = new ColumnFamilyOptions().optimizeForSmallDb();
        this.dbOptions = new DBOptions().setCreateIfMissing(true)
            .setCreateMissingColumnFamilies(true);
        columnFamilyNameHandleMap = new ConcurrentHashMap<>();
        File storeFile = new File(this.path);
        if (!storeFile.getParentFile().exists()) {
            if (!storeFile.getParentFile().mkdirs()) {
                throw new RocksDBException("Failed to create directory: " + storeFile.getParentFile().getAbsolutePath());
            }
        }
        List<byte[]> columnFamilyNames = new ArrayList<>();
        List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        if (storeFile.exists()) {
            columnFamilyNames.addAll(RocksDB.listColumnFamilies(new Options(dbOptions, columnFamilyOptions),
                this.path));
        } else {
            columnFamilyNames.add(RocksDB.DEFAULT_COLUMN_FAMILY);
        }
        for (byte[] columnFamilyName : columnFamilyNames) {
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor(columnFamilyName, columnFamilyOptions));
        }
        rocksDB = RocksDB.open(dbOptions, this.path, columnFamilyDescriptors, columnFamilyHandles);
        for (int i = 0; i < columnFamilyNames.size(); i++) {
            columnFamilyNameHandleMap.put(new String(columnFamilyNames.get(i)),
                columnFamilyHandles.get(i));
        }
    }

    @Override
    public byte[] get(final String namespace, final byte[] key) throws RocksDBException {
        if (stopped) {
            throw new RocksDBException("KV service is stopped.");
        }

        if (!columnFamilyNameHandleMap.containsKey(namespace)) {
            return null;
        }

        ColumnFamilyHandle handle = columnFamilyNameHandleMap.get(namespace);
        return rocksDB.get(handle, key);
    }

    private boolean checkPrefix(byte[] key, byte[] upperBound) {
        if (key.length < upperBound.length) {
            return false;
        }
        for (int i = 0; i < upperBound.length; i++) {
            if (key[i] > upperBound[i]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void iterate(final String namespace, IteratorCallback callback) throws RocksDBException {
        if (stopped) {
            throw new RocksDBException("KV service is stopped.");
        }

        if (callback == null) {
            throw new RocksDBException("The callback can not be null.");
        }

        ColumnFamilyHandle columnFamilyHandle = columnFamilyNameHandleMap.get(namespace);
        if (columnFamilyHandle == null) {
            return;
        }
        try (RocksIterator iterator = rocksDB.newIterator(columnFamilyHandle)) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                callback.onRead(iterator.key(), iterator.value());
            }
            iterator.status();
        }
    }

    @Override
    public void iterate(final String namespace, final byte[] prefix, final byte[] start,
        final byte[] end, IteratorCallback callback) throws RocksDBException {
        if (stopped) {
            throw new RocksDBException("KV service is stopped.");
        }

        if (callback == null) {
            throw new RocksDBException("The callback can not be null.");
        }

        if (Objects.isNull(prefix) && Objects.isNull(start)) {
            throw new RocksDBException("To determine lower bound, prefix and start may not be null at the same time.");
        }

        if (Objects.isNull(prefix) && Objects.isNull(end)) {
            throw new RocksDBException("To determine upper bound, prefix and end may not be null at the same time.");
        }

        ColumnFamilyHandle columnFamilyHandle = columnFamilyNameHandleMap.get(namespace);
        if (columnFamilyHandle == null) {
            return;
        }

        ReadOptions readOptions = null;
        Slice startSlice = null;
        Slice endSlice = null;
        Slice prefixSlice = null;
        RocksIterator iterator = null;
        try {
            readOptions = new ReadOptions();
            readOptions.setTotalOrderSeek(true);
            readOptions.setReadaheadSize(4L * 1024 * 1024);
            boolean hasStart = !Objects.isNull(start);
            boolean hasPrefix = !Objects.isNull(prefix);

            if (hasStart) {
                startSlice = new Slice(start);
                readOptions.setIterateLowerBound(startSlice);
            } else {
                prefixSlice = new Slice(prefix);
                readOptions.setIterateLowerBound(prefixSlice);
            }

            if (!Objects.isNull(end)) {
                endSlice = new Slice(end);
                readOptions.setIterateUpperBound(endSlice);
            }

            iterator = rocksDB.newIterator(columnFamilyHandle, readOptions);
            if (hasStart) {
                iterator.seek(start);
            } else if (hasPrefix) {
                iterator.seek(prefix);
            }

            while (iterator.isValid()) {
                byte[] key = iterator.key();
                if (hasPrefix && !checkPrefix(key, prefix)) {
                    break;
                }
                callback.onRead(iterator.key(), iterator.value());
                iterator.next();
            }
        } finally {
            if (startSlice != null) {
                startSlice.close();
            }
            if (endSlice != null) {
                endSlice.close();
            }
            if (prefixSlice != null) {
                prefixSlice.close();
            }
            if (readOptions != null) {
                readOptions.close();
            }
            if (iterator != null) {
                iterator.close();
            }
        }
    }

    private ColumnFamilyHandle getOrCreateColumnFamily(String columnFamily) throws RocksDBException {
        if (!columnFamilyNameHandleMap.containsKey(columnFamily)) {
            synchronized (this) {
                if (!columnFamilyNameHandleMap.containsKey(columnFamily)) {
                    ColumnFamilyDescriptor columnFamilyDescriptor =
                        new ColumnFamilyDescriptor(columnFamily.getBytes(), columnFamilyOptions);
                    ColumnFamilyHandle columnFamilyHandle = rocksDB.createColumnFamily(columnFamilyDescriptor);
                    columnFamilyNameHandleMap.putIfAbsent(columnFamily, columnFamilyHandle);
                }
            }
        }
        return columnFamilyNameHandleMap.get(columnFamily);
    }

    @Override
    public void put(final String namespace, byte[] key, byte[] value) throws RocksDBException {
        if (stopped) {
            throw new RocksDBException("KV service is stopped.");
        }

        ColumnFamilyHandle handle = getOrCreateColumnFamily(namespace);
        rocksDB.put(handle, key, value);
    }

    @Override
    public void batch(BatchRequest... requests) throws RocksDBException {
        if (stopped) {
            throw new RocksDBException("KV service is stopped.");
        }

        if (requests == null) {
            throw new RocksDBException("The requests can not be null.");
        }

        try (WriteOptions writeOptions = new WriteOptions(); WriteBatch writeBatch = new WriteBatch()) {
            for (BatchRequest request : requests) {
                ColumnFamilyHandle handle = getOrCreateColumnFamily(request.namespace());
                switch (request.type()) {
                    case WRITE -> writeBatch.put(handle, request.key(), request.value());
                    case DELETE -> writeBatch.delete(handle, request.key());
                    default -> throw new RocksDBException("Unsupported request type: " + request.type());
                }
            }
            rocksDB.write(writeOptions, writeBatch);
        }
    }

    @Override
    public void delete(final String namespace, byte[] key) throws RocksDBException {
        if (stopped) {
            throw new RocksDBException("KV service is stopped.");
        }
        if (columnFamilyNameHandleMap.containsKey(namespace)) {
            ColumnFamilyHandle handle = columnFamilyNameHandleMap.get(namespace);
            rocksDB.delete(handle, key);
        }
    }

    @Override
    public void flush(boolean sync) throws RocksDBException {
        if (stopped) {
            throw new RocksDBException("KV service is stopped.");
        }
        rocksDB.flushWal(sync);
    }

    @Override
    public void close() throws RocksDBException {
        if (stopped) {
            return;
        }
        stopped = true;
        rocksDB.flushWal(true);
        for (Map.Entry<String, ColumnFamilyHandle> entry : columnFamilyNameHandleMap.entrySet()) {
            entry.getValue().close();
        }
        rocksDB.closeE();
        dbOptions.close();
        columnFamilyOptions.close();
    }

    @Override
    public void destroy() throws RocksDBException {
        close();
        RocksDB.destroyDB(path, new Options());
    }
}
