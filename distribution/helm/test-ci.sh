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

VERSION=$1
TEST_CODE_GIT=$2
TEST_CODE_BRANCH=$3
TEST_CODE_PATH=$4
TEST_CMD_BASE=$5

ASK_CONFIG=$6
JOB_INDEX=$7

export REPO_NAME=`echo ${GITHUB_REPOSITORY#*/} | sed -e "s/\//-/g" | cut -c1-36 | tr '[A-Z]' '[a-z]'`
export WORKFLOW_NAME=${GITHUB_WORKFLOW}
export RUN_ID=${GITHUB_RUN_ID}
export TEST_CODE_GIT
export TEST_CODE_BRANCH
export TEST_CODE_PATH

echo "Start test version: ${GITHUB_REPOSITORY}@${VERSION}"

echo "************************************"
echo "*          Set config...           *"
echo "************************************"
mkdir -p ${HOME}/.kube
kube_config=$(echo "${ASK_CONFIG}" | base64 -d)
echo "${kube_config}" > ${HOME}/.kube/config
export KUBECONFIG="${HOME}/.kube/config"

env_uuid=${REPO_NAME}-${GITHUB_RUN_ID}-${JOB_INDEX}

echo "************************************"
echo "*        E2E Test local...         *"
echo "************************************"

wget https://dlcdn.apache.org/maven/maven-3/3.8.7/binaries/apache-maven-3.8.7-bin.tar.gz
tar -zxvf apache-maven-3.8.7-bin.tar.gz -C /opt/
export PATH=$PATH:/opt/apache-maven-3.8.7/bin

ns=${env_uuid}

echo namespace: $ns
echo $TEST_CODE_GIT
echo $TEST_CMD_BASE

TEST_CMD=`echo "${TEST_CMD_BASE}" | sed -s 's/^/        /g'`

git clone $TEST_CODE_GIT -b $TEST_CODE_BRANCH code

cd code
cd $TEST_CODE_PATH
${TEST_CMD}
exit_code=$?

killall kubectl
exit ${exit_code}