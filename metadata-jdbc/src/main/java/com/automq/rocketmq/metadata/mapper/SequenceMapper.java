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

package com.automq.rocketmq.metadata.mapper;

import org.apache.ibatis.annotations.Param;

public interface SequenceMapper {

    /**
     * Create a new sequence
     *
     * @param name The sequence name
     * @param next initial next value
     * @return rows affected
     */
    int create(@Param("name") String name, @Param("next") long next);

    /**
     * Get the next value of the sequence with write lock held.
     * See <a href="https://dev.mysql.com/doc/refman/8.0/en/innodb-locking-reads.html">Locking Reads</a>
     *
     * @param name Sequence name
     * @return Next value of the given sequence name
     */
    long next(@Param("name") String name);

    int update(@Param("name") String name, @Param("next") long next);
}
