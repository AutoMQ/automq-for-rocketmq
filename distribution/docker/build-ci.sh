#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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