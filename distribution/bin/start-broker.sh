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
DIST_DIR="$(dirname "$SCRIPT_DIR")"
"$SCRIPT_DIR"/run-server.sh com.automq.rocketmq.broker.BrokerStartup -c "$DIST_DIR/conf/broker.yaml"