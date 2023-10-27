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
CREATE TABLE IF NOT EXISTS lease
(
    node_id         INT      NOT NULL,
    epoch           INT      NOT NULL DEFAULT 0,
    expiration_time DATETIME NOT NULL
);

INSERT INTO lease(node_id, epoch, expiration_time)
VALUES (0, 0, TIMESTAMP('2023-01-01'));

CREATE TABLE IF NOT EXISTS node
(
    id          INT          NOT NULL PRIMARY KEY AUTO_INCREMENT,
    name        VARCHAR(255) NOT NULL,
    instance_id VARCHAR(255),
    volume_id   VARCHAR(255),
    hostname    VARCHAR(255),
    vpc_id      VARCHAR(255),
    address     VARCHAR(255) NOT NULL,
    epoch       INT          NOT NULL DEFAULT 0,
    create_time DATETIME              DEFAULT CURRENT_TIMESTAMP,
    update_time DATETIME              DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE INDEX idx_name (name),
    UNIQUE INDEX idx_host_name (hostname),
    UNIQUE INDEX idx_address (address)
);

CREATE TABLE IF NOT EXISTS topic
(
    id                   BIGINT       NOT NULL PRIMARY KEY AUTO_INCREMENT,
    name                 VARCHAR(255) NOT NULL,
    queue_num            INT          NOT NULL DEFAULT 0,
    status               TINYINT               DEFAULT 0,
    create_time          DATETIME              DEFAULT current_timestamp,
    update_time          DATETIME              DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    accept_message_types TEXT         NOT NULL,
    UNIQUE INDEX idx_topic_name (name)
);

CREATE TABLE IF NOT EXISTS queue_assignment
(
    topic_id    BIGINT  NOT NULL,
    queue_id    INT     NOT NULL,
    src_node_id INT     NOT NULL,
    dst_node_id INT     NOT NULL,
    status      TINYINT NOT NULL,
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stream
(
    id           BIGINT    NOT NULL PRIMARY KEY AUTO_INCREMENT,
    topic_id     BIGINT    NOT NULL,
    queue_id     INT       NOT NULL,
    stream_role  TINYINT   NOT NULL DEFAULT 0,
    group_id     BIGINT    NULL,
    src_node_id  INT,
    dst_node_id  INT,
    epoch        BIGINT    NOT NULL DEFAULT -1,
    range_id     INT       NOT NULL DEFAULT -1,
    start_offset BIGINT    NOT NULL DEFAULT 0,
    state        INT       NOT NULL DEFAULT 1,
    create_time  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE INDEX idx_queue (topic_id, queue_id, group_id, stream_role)
);

CREATE TABLE IF NOT EXISTS consumer_group
(
    id                   BIGINT       NOT NULL PRIMARY KEY AUTO_INCREMENT,
    name                 VARCHAR(255) NOT NULL,
    status               TINYINT      NOT NULL DEFAULT 0,
    dead_letter_topic_id BIGINT,
    max_delivery_attempt INT          NOT NULL DEFAULT 16,
    group_type           TINYINT      NOT NULL,
    create_time          DATETIME              DEFAULT CURRENT_TIMESTAMP,
    update_time          DATETIME              DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE INDEX idx_name (name)
);

CREATE TABLE IF NOT EXISTS subscription
(
    id          BIGINT       NOT NULL PRIMARY KEY AUTO_INCREMENT,
    group_id    BIGINT       NOT NULL,
    topic_id    BIGINT       NOT NULL,
    expression  VARCHAR(255) NOT NULL,
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE INDEX idx_subscription_group_topic (group_id, topic_id)
);

CREATE TABLE IF NOT EXISTS group_progress
(
    group_id     BIGINT NOT NULL,
    topic_id     BIGINT NOT NULL,
    queue_id     INT    NOT NULL,
    queue_offset BIGINT NOT NULL,
    UNIQUE INDEX idx_group_progress (group_id, topic_id, queue_id)
);

CREATE TABLE IF NOT EXISTS `range`
(
    id           BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    range_id     INT    NOT NULL,
    stream_id    BIGINT NOT NULL,
    epoch        BIGINT NOT NULL,
    start_offset BIGINT NOT NULL,
    end_offset   BIGINT NOT NULL,
    node_id      INT    NOT NULL,
    UNIQUE INDEX idx_stream_range (stream_id, range_id),
    INDEX idx_stream_start_offset (stream_id, start_offset)
);

CREATE TABLE IF NOT EXISTS s3object
(
    id                            BIGINT  NOT NULL PRIMARY KEY,
    object_size                   BIGINT,
    stream_id                     BIGINT,
    prepared_timestamp            TIMESTAMP(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    committed_timestamp           TIMESTAMP(3),
    expired_timestamp             TIMESTAMP(3)  NOT NULL,
    marked_for_deletion_timestamp TIMESTAMP(3),
    state                         TINYINT NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS s3streamobject
(
    id                  BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    stream_id           BIGINT NOT NULL,
    start_offset        BIGINT NOT NULL,
    end_offset          BIGINT NOT NULL,
    object_id           BIGINT NOT NULL,
    object_size         BIGINT NOT NULL,
    base_data_timestamp BIGINT,
    committed_timestamp BIGINT,
    created_timestamp   BIGINT,
    UNIQUE INDEX uk_s3_stream_object_object_id (object_id),
    INDEX idx_s3_stream_object_stream_id (stream_id, start_offset)
);

CREATE TABLE IF NOT EXISTS s3walobject
(
    object_id           BIGINT   NOT NULL,
    object_size         BIGINT   NOT NULL,
    node_id             INT      NOT NULL,
    sequence_id         BIGINT   NOT NULL,
    sub_streams         LONGTEXT NOT NULL, -- immutable
    base_data_timestamp BIGINT,
    committed_timestamp BIGINT,
    created_timestamp   BIGINT,
    UNIQUE INDEX uk_s3_wal_object_node_sequence_id (node_id, sequence_id),
    INDEX idx_s3_wal_object_object_id (object_id)
);

CREATE TABLE IF NOT EXISTS sequence (
    name VARCHAR(255) NOT NULL,
    next BIGINT NOT NULL DEFAULT 1,
    UNIQUE INDEX idx_name(name)
);
INSERT INTO sequence(name) VALUES ('S3_OBJECT_ID_SEQ');