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

package com.automq.rocketmq.store;

import com.automq.rocketmq.store.api.S3ObjectOperator;
import com.automq.stream.s3.operator.S3Operator;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyList;

public class S3ObjectOperatorTest {

    private S3ObjectOperator operator;
    private S3Operator s3Operator;

    @BeforeEach
    public void setUp() {
        s3Operator = Mockito.mock(S3Operator.class);
        operator = new S3ObjectOperatorImpl(s3Operator);
    }

    @Test
    public void delete_normal() {
        Mockito.doAnswer(ink -> CompletableFuture.completedFuture(ink.getArgument(0)))
            .when(s3Operator).delete(Mockito.anyList());
        List<Long> deletedKeys = operator.delete(List.of(1L, 2L, 3L)).join();
        assertEquals(List.of(1L, 2L, 3L), deletedKeys);
    }

    @Test
    public void delete_multiple_batch() {
        Mockito.doAnswer(ink -> {
            List<String> objectKeys = ink.getArgument(0);
            assertEquals(800, objectKeys.size());
            List deletedKeys = new ArrayList<>(objectKeys);
            deletedKeys.remove(0);
            return CompletableFuture.completedFuture(deletedKeys);
        }).doAnswer(ink -> {
            List<String> objectKeys = ink.getArgument(0);
            assertEquals(800, objectKeys.size());
            return CompletableFuture.completedFuture(objectKeys);
        }).doAnswer(ink -> {
            List<String> objectKeys = ink.getArgument(0);
            assertEquals(100, objectKeys.size());
            return CompletableFuture.completedFuture(objectKeys);
        }).when(s3Operator).delete(anyList());

        List<Long> keys = Stream.iterate(0L, i -> i + 1).limit(1700).toList();

        List<Long> deletedKeys = operator.delete(keys).join();
        assertEquals(1699, deletedKeys.size());
        assertEquals(Stream.iterate(1L, i -> i + 1).limit(1699).toList(), deletedKeys);
        Mockito.verify(s3Operator, Mockito.times(3)).delete(anyList());
    }


}
