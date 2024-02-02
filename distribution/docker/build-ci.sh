#!/bin/bash

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

ROCKETMQ_REPO=$1
ROCKETMQ_VERSION=$2
GITHUB_REPOSITORY='https://github.com/AutoMQ/automq-for-rocketmq.git '

getLatestReleaseVersion() {
  full_node_version=`git ls-remote --tags $GITHUB_REPOSITORY | awk -F '/' 'END{print $3}'`
  commit_version=`git rev-parse --short HEAD`
  if [[ -n $full_node_version ]]; then
   echo $full_node_version-$commit_version
  else
   echo "latest"-$commit_version
  fi
}

docker_build() {
  docker build --no-cache -f Dockerfile-ci -t ${ROCKETMQ_REPO}:${ROCKETMQ_VERSION} --build-arg version=${ROCKETMQ_VERSION} . --progress=plain
}

if [[ -z $ROCKETMQ_REPO ]]; then
  ROCKETMQ_REPO="automq-for-rocketmq"
  echo "ROCKETMQ_REPO is empty, use default repo: $ROCKETMQ_REPO"
fi
echo "info: ROCKETMQ_REPO: $ROCKETMQ_REPO"
if [[ -z $ROCKETMQ_VERSION ]]; then
  ROCKETMQ_VERSION=$(`echo getLatestReleaseVersion`)
  echo "ROCKETMQ_VERSION is empty, use latest version: $ROCKETMQ_VERSION"
fi

echo "start docker build automq-for-rocketmq version: $ROCKETMQ_VERSION"
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
REPO_DIR=$(dirname "$(dirname "$SCRIPT_DIR")")
cd "$SCRIPT_DIR" || exit 1
pwd
if [ ! -d "rocketmq" ]; then
  zip_file="$REPO_DIR/distribution/target/automq-for-rocketmq.zip"
  if [ -f "$zip_file" ]; then
    cp "$zip_file" .
    unzip -q ./automq-for-rocketmq.zip
    mv automq-for-rocketmq rocketmq
  else
    echo "Please 'mvn package' to package automq-for-rocketmq.zip first"
    exit 1
  fi
fi

docker_build