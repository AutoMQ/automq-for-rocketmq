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
