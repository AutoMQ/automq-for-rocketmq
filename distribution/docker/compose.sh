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
cd "$SCRIPT_DIR" || exit 1
cp "$REPO_DIR/metadata-jdbc/src/main/resources/ddl.sql" .

OS_NAME=$(uname)
if [ "$OS_NAME" == "Linux" ]
then
  IP=$(ip -o route get to 8.8.8.8 | sed -n 's/.*src \([0-9.]\+\).*/\1/p')
elif [ "$OS_NAME" == "Darwin" ]; then
  IP=$(ipconfig getifaddr en0)
fi
export EXTERNAL_IP="$IP"

docker compose up