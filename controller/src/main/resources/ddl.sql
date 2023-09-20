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
CREATE TABLE IF NOT EXISTS lease (
    node_id INT NOT NULL ,
    epoch INT NOT NULL DEFAULT 0,
    expiration_time DATETIME NOT NULL
);

INSERT INTO lease(node_id, epoch, expiration_time) VALUES (0, 0, TIMESTAMP('2023-01-01'));

CREATE TABLE IF NOT EXISTS node (
    id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL,
    instance_id VARCHAR(255),
    volume_id VARCHAR(255),
    hostname VARCHAR(255),
    vpc_id VARCHAR(255),
    address VARCHAR(255) NOT NULL,
    epoch INT NOT NULL DEFAULT 1,
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS topic (
    id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL,
    queue_num INT NOT NULL ,
    deleted BOOL DEFAULT FALSE,
    create_time DATETIME DEFAULT current_timestamp,
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS queue_assignment (
    topic_id INT NOT NULL ,
    queue_id INT NOT NULL ,
    src_node_id INT NOT NULL ,
    dst_node_id INT NOT NULL ,
    status TINYINT NOT NULL ,
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE  CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS queue (
    topic_id INT NOT NULL ,
    queue_id INT NOT NULL ,
    stream_id INT NOT NULL ,
    role TINYINT NOT NULL
);

CREATE TABLE IF NOT EXISTS consumer_group (
    id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL ,
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS subscription (
    id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    group_id INT NOT NULL ,
    topic_id INT NOT NULL ,
    expression VARCHAR(255) NOT NULL,
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);