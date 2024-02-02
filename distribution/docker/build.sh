#!/usr/bin/env bash
#
# Copyright 2024, AutoMQ CO.,LTD.
#
# Use of this software is governed by the Business Source License
# included in the file BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
#

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
REPO_DIR=$(dirname "$(dirname "$SCRIPT_DIR")")
cd "$REPO_DIR" || exit 1
mvn package -Dmaven.test.skip=true
cp distribution/target/automq-for-rocketmq.tar.gz "$SCRIPT_DIR"
cd "$SCRIPT_DIR" || exit 1
docker build -t automqinc/automq-for-rocketmq:v0.0.3-alpha -f Dockerfile .


