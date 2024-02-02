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

package com.automq.rocketmq.store;

import com.automq.rocketmq.store.api.S3ObjectOperator;
import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.operator.S3Operator;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3ObjectOperatorImpl implements S3ObjectOperator {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3ObjectOperatorImpl.class);
    public static final int MAX_BATCH_DELETE_SIZE = 800;
    private final S3Operator operator;

    public S3ObjectOperatorImpl(S3Operator operator) {
        this.operator = operator;
    }

    @Override
    public CompletableFuture<List<Long>> delete(List<Long> objectIds) {
        List<String> objectKeys = objectIds
            .stream()
            .map(id -> ObjectUtils.genKey(0, id))
            .collect(Collectors.toList());
        return delete0(objectKeys).thenApply(deletedKeys ->
            deletedKeys
                .stream()
                .map(key -> ObjectUtils.parseObjectId(0, key))
                .collect(Collectors.toList())
        );
    }

    private CompletableFuture<List<String>> delete0(List<String> objectKeys) {
        List<CompletableFuture<List<String>>> deleteCfs = new ArrayList<>();
        for (int i = 0; i < objectKeys.size() / MAX_BATCH_DELETE_SIZE; i++) {
            List<String> batch = objectKeys.subList(i * MAX_BATCH_DELETE_SIZE, (i + 1) * MAX_BATCH_DELETE_SIZE);
            deleteCfs.add(operator.delete(batch));
        }
        if (objectKeys.size() % MAX_BATCH_DELETE_SIZE != 0) {
            List<String> batch = objectKeys.subList(objectKeys.size() / MAX_BATCH_DELETE_SIZE * MAX_BATCH_DELETE_SIZE, objectKeys.size());
            deleteCfs.add(operator.delete(batch));
        }
        return CompletableFuture.allOf(deleteCfs.toArray(new CompletableFuture[0]))
            .thenApply(nil -> deleteCfs
                .stream()
                .map(CompletableFuture::join)
                .flatMap(List::stream)
                .collect(Collectors.toList()));
    }

}
