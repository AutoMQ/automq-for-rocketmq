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

package com.automq.rocketmq.store.service;

import com.automq.rocketmq.store.exception.StoreErrorCode;
import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.model.kv.BatchRequest;
import com.automq.rocketmq.store.model.kv.IteratorCallback;
import com.automq.rocketmq.store.model.kv.KVReadOptions;
import com.automq.rocketmq.store.service.api.KVService;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.commons.lang3.StringUtils;
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
import org.rocksdb.Snapshot;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

public class RocksDBKVService implements KVService {
    private final String path;
    private final ColumnFamilyOptions columnFamilyOptions;
    private final DBOptions dbOptions;
    private final ConcurrentMap<String, ColumnFamilyHandle> columnFamilyNameHandleMap;
    private final ConcurrentMap<Long, Snapshot> snapshotMap;
    private final RocksDB rocksDB;
    private volatile boolean stopped;

    public RocksDBKVService(String path) throws StoreException {
        this.path = path;
        this.columnFamilyOptions = new ColumnFamilyOptions().optimizeForSmallDb();
        this.dbOptions = new DBOptions().setCreateIfMissing(true)
            .setCreateMissingColumnFamilies(true);
        columnFamilyNameHandleMap = new ConcurrentHashMap<>();
        File storeFile = new File(this.path);
        if (!storeFile.getParentFile().exists()) {
            if (!storeFile.getParentFile().mkdirs()) {
                throw new StoreException(StoreErrorCode.FILE_SYSTEM_PERMISSION, "Failed to create directory: " + storeFile.getParentFile().getAbsolutePath());
            }
        }
        List<byte[]> columnFamilyNames = new ArrayList<>();
        List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        if (storeFile.exists()) {
            transformException(() -> columnFamilyNames.addAll(RocksDB.listColumnFamilies(new Options(dbOptions, columnFamilyOptions), this.path)),
                "Failed to list column families.");

        } else {
            columnFamilyNames.add(RocksDB.DEFAULT_COLUMN_FAMILY);
        }
        for (byte[] columnFamilyName : columnFamilyNames) {
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor(columnFamilyName, columnFamilyOptions));
        }

        rocksDB = transformException(() -> RocksDB.open(dbOptions, this.path, columnFamilyDescriptors, columnFamilyHandles),
            "Failed to open RocksDB.");
        for (int i = 0; i < columnFamilyNames.size(); i++) {
            columnFamilyNameHandleMap.put(new String(columnFamilyNames.get(i)),
                columnFamilyHandles.get(i));
        }
        snapshotMap = new ConcurrentHashMap<>();
    }

    protected interface RocksDBSupplier<T> {
        T execute() throws RocksDBException;
    }

    protected static <T> T transformException(RocksDBSupplier<T> operation, String message) throws StoreException {
        try {
            return operation.execute();
        } catch (RocksDBException e) {
            if (StringUtils.isBlank(message)) {
                throw new StoreException(StoreErrorCode.KV_ENGINE_ERROR, message, e);
            } else {
                throw new StoreException(StoreErrorCode.KV_ENGINE_ERROR, e.getMessage(), e);
            }
        }
    }

    protected interface RocksDBOperation {
        void execute() throws RocksDBException;
    }

    protected static void transformException(RocksDBOperation operation, String message) throws StoreException {
        try {
            operation.execute();
        } catch (RocksDBException e) {
            if (StringUtils.isBlank(message)) {
                throw new StoreException(StoreErrorCode.KV_ENGINE_ERROR, message, e);
            } else {
                throw new StoreException(StoreErrorCode.KV_ENGINE_ERROR, e.getMessage(), e);
            }
        }
    }

    @Override
    public byte[] get(final String namespace, final byte[] key,
        final KVReadOptions kvReadOptions) throws StoreException {
        if (stopped) {
            throw new StoreException(StoreErrorCode.KV_SERVICE_IS_NOT_RUNNING, "KV service is stopped.");
        }

        if (!columnFamilyNameHandleMap.containsKey(namespace)) {
            return null;
        }
        try (ReadOptions readOptions = new ReadOptions()) {
            transformKVReadOptions(readOptions, kvReadOptions);
            ColumnFamilyHandle handle = columnFamilyNameHandleMap.get(namespace);
            return transformException(() -> rocksDB.get(handle, readOptions, key)
                , "Failed to get value from RocksDB.");
        }
    }

    @Override
    public byte[] getByPrefix(String namespace, final byte[] prefix) throws StoreException {
        if (stopped) {
            throw new StoreException(StoreErrorCode.KV_SERVICE_IS_NOT_RUNNING, "KV service is stopped.");
        }

        if (!columnFamilyNameHandleMap.containsKey(namespace)) {
            return null;
        }

        ColumnFamilyHandle handle = columnFamilyNameHandleMap.get(namespace);
        try (RocksIterator iterator = rocksDB.newIterator(handle)) {
            iterator.seek(prefix);
            if (!iterator.isValid()) {
                return null;
            }
            byte[] key = iterator.key();
            if (!checkPrefix(key, prefix)) {
                return null;
            }
            return iterator.value();
        }
    }

    private void transformKVReadOptions(ReadOptions readOptions, KVReadOptions kvReadOptions) {
        if (kvReadOptions.isSnapshotRead()) {
            readOptions.setSnapshot(snapshotMap.get(kvReadOptions.getSnapshotVersion()));
        }
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
    public void iterate(final String namespace, IteratorCallback callback,
        KVReadOptions kvReadOptions) throws StoreException {
        if (stopped) {
            throw new StoreException(StoreErrorCode.KV_SERVICE_IS_NOT_RUNNING, "KV service is stopped.");
        }

        if (callback == null) {
            throw new StoreException(StoreErrorCode.ILLEGAL_ARGUMENT, "The callback can not be null.");
        }

        ColumnFamilyHandle columnFamilyHandle = columnFamilyNameHandleMap.get(namespace);
        if (columnFamilyHandle == null) {
            return;
        }
        try (ReadOptions readOptions = new ReadOptions()) {
            transformKVReadOptions(readOptions, kvReadOptions);
            try (RocksIterator iterator = rocksDB.newIterator(columnFamilyHandle, readOptions)) {
                for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                    callback.onRead(iterator.key(), iterator.value());
                }
            }
        }
    }

    @Override
    public void iterate(final String namespace, final byte[] prefix, final byte[] start,
        final byte[] end, IteratorCallback callback, KVReadOptions kvReadOptions) throws StoreException {
        if (stopped) {
            throw new StoreException(StoreErrorCode.KV_SERVICE_IS_NOT_RUNNING, "KV service is stopped.");
        }

        if (callback == null) {
            throw new StoreException(StoreErrorCode.ILLEGAL_ARGUMENT, "The callback can not be null.");
        }

        if (Objects.isNull(prefix) && Objects.isNull(start)) {
            throw new StoreException(StoreErrorCode.ILLEGAL_ARGUMENT, "To determine lower bound, prefix and start may not be null at the same time.");
        }

        if (Objects.isNull(prefix) && Objects.isNull(end)) {
            throw new StoreException(StoreErrorCode.ILLEGAL_ARGUMENT, "To determine upper bound, prefix and end may not be null at the same time.");
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
            transformKVReadOptions(readOptions, kvReadOptions);
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
            } else {
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

    private ColumnFamilyHandle getOrCreateColumnFamily(String columnFamily) throws StoreException {
        if (!columnFamilyNameHandleMap.containsKey(columnFamily)) {
            synchronized (this) {
                if (!columnFamilyNameHandleMap.containsKey(columnFamily)) {
                    ColumnFamilyDescriptor columnFamilyDescriptor =
                        new ColumnFamilyDescriptor(columnFamily.getBytes(), columnFamilyOptions);
                    ColumnFamilyHandle columnFamilyHandle = transformException(() -> rocksDB.createColumnFamily(columnFamilyDescriptor),
                        "Failed to create column family.");
                    columnFamilyNameHandleMap.putIfAbsent(columnFamily, columnFamilyHandle);
                }
            }
        }
        return columnFamilyNameHandleMap.get(columnFamily);
    }

    @Override
    public void put(final String namespace, byte[] key, byte[] value) throws StoreException {
        if (stopped) {
            throw new StoreException(StoreErrorCode.KV_SERVICE_IS_NOT_RUNNING, "KV service is stopped.");
        }
        try (WriteOptions writeOptions = new WriteOptions().setDisableWAL(true)) {
            ColumnFamilyHandle handle = getOrCreateColumnFamily(namespace);
            transformException(() -> rocksDB.put(handle, writeOptions, key, value),
                "Failed to put value into RocksDB.");
        }
    }

    @Override
    public void batch(BatchRequest... requests) throws StoreException {
        if (stopped) {
            throw new StoreException(StoreErrorCode.KV_SERVICE_IS_NOT_RUNNING, "KV service is stopped.");
        }

        if (requests == null) {
            throw new StoreException(StoreErrorCode.ILLEGAL_ARGUMENT, "The requests can not be null.");
        }

        try (WriteOptions writeOptions = new WriteOptions(); WriteBatch writeBatch = new WriteBatch()) {
            for (BatchRequest request : requests) {
                ColumnFamilyHandle handle = getOrCreateColumnFamily(request.namespace());
                switch (request.type()) {
                    case WRITE -> transformException(() -> writeBatch.put(handle, request.key(), request.value()),
                        "Failed to put value into RocksDB.");
                    case DELETE -> transformException(() -> writeBatch.delete(handle, request.key()),
                        "Failed to delete value from RocksDB.");
                    default ->
                        throw new StoreException(StoreErrorCode.ILLEGAL_ARGUMENT, "Unsupported request type: " + request.type());
                }
            }
            transformException(() -> rocksDB.write(writeOptions, writeBatch),
                "Failed to batch write into RocksDB.");
        }
    }

    @Override
    public void delete(final String namespace, byte[] key) throws StoreException {
        if (stopped) {
            throw new StoreException(StoreErrorCode.KV_SERVICE_IS_NOT_RUNNING, "KV service is stopped.");
        }
        if (columnFamilyNameHandleMap.containsKey(namespace)) {
            ColumnFamilyHandle handle = columnFamilyNameHandleMap.get(namespace);
            transformException(() -> rocksDB.delete(handle, key),
                "Failed to delete value from RocksDB.");
        }
    }

    @Override
    public void clear(String namespace) throws StoreException {
        if (stopped) {
            throw new StoreException(StoreErrorCode.KV_SERVICE_IS_NOT_RUNNING, "KV service is stopped.");
        }
        if (columnFamilyNameHandleMap.containsKey(namespace)) {
            ColumnFamilyHandle handle = columnFamilyNameHandleMap.get(namespace);
            transformException(() -> rocksDB.dropColumnFamily(handle),
                "Failed to drop column family.");
            ColumnFamilyHandle nativeHandle = columnFamilyNameHandleMap.remove(namespace);
            if (nativeHandle != null) {
                nativeHandle.close();
            }
        }
    }

    @Override
    public void flush(boolean sync) throws StoreException {
        if (stopped) {
            throw new StoreException(StoreErrorCode.KV_SERVICE_IS_NOT_RUNNING, "KV service is stopped.");
        }
        transformException(() -> rocksDB.flushWal(sync),
            "Failed to flush RocksDB.");
    }

    @Override
    public void close() throws StoreException {
        if (stopped) {
            return;
        }
        stopped = true;
        transformException(() -> rocksDB.flushWal(true), "Failed to flush RocksDB.");
        for (Map.Entry<String, ColumnFamilyHandle> entry : columnFamilyNameHandleMap.entrySet()) {
            entry.getValue().close();
        }
        transformException(rocksDB::closeE, "Failed to close RocksDB.");
        dbOptions.close();
        columnFamilyOptions.close();
    }

    @Override
    public void destroy() throws StoreException {
        close();
        transformException(() -> RocksDB.destroyDB(path, new Options()),
            "Failed to destroy RocksDB.");
    }

    @Override
    public long takeSnapshot() throws StoreException {
        if (stopped) {
            throw new StoreException(StoreErrorCode.KV_SERVICE_IS_NOT_RUNNING, "KV service is stopped.");
        }
        Snapshot snapshot = rocksDB.getSnapshot();
        long snapshotVersion = snapshot.getSequenceNumber();
        snapshotMap.put(snapshotVersion, snapshot);
        return snapshotVersion;
    }

    @Override
    public void releaseSnapshot(long snapshotVersionId) throws StoreException {
        if (stopped) {
            throw new StoreException(StoreErrorCode.KV_SERVICE_IS_NOT_RUNNING, "KV service is stopped.");
        }
        Snapshot snapshot = snapshotMap.remove(snapshotVersionId);
        if (snapshot != null) {
            rocksDB.releaseSnapshot(snapshot);
            snapshot.close();
        }
    }
}
