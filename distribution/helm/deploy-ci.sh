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
NAMESPACE=$3

if [[ -z $NAMESPACE ]]; then
  NAMESPACE="default"
  echo "info: NAMESPACE is empty, use default namespace: $NAMESPACE"
fi

echo "************************************"
echo "*          init config...          *"
echo "************************************"

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
REPO_DIR=$(dirname "$(dirname "$SCRIPT_DIR")")
echo "SCRIPT_DIR: $SCRIPT_DIR"
echo "REPO_DIR: $REPO_DIR"
cd "$SCRIPT_DIR" || exit 1
if [ ! -f "deploy/ddl.sql" ]
then
  cp "$REPO_DIR/metadata-jdbc/src/main/resources/ddl.sql" deploy/
fi

# make ddl to configmap
if [ ! -f "deploy/init-db-configmap.yaml" ]
then
  cp deploy/configmap-template.yaml deploy/init-db-configmap.yaml
  sed 's/^/    /' deploy/ddl.sql >> deploy/init-db-configmap.yaml
fi

kubectl apply -f deploy/init-db-configmap.yaml --namespace $NAMESPACE

echo "************************************"
echo "*     Create env and deploy...     *"
echo "************************************"

echo ${ROCKETMQ_REPO}:${ROCKETMQ_VERSION}: ${NAMESPACE} deploy start

# deploy s3-localstack
helm repo add localstack-charts https://localstack.github.io/helm-charts
helm install s3-localstack localstack-charts/localstack -f deploy/localstack_s3.yaml --namespace $NAMESPACE

# Wait for s3-localstack to be ready
kubectl rollout status --watch --timeout=120s statefulset/s3-localstack --namespace $NAMESPACE

# deploy mysql
helm repo add bitnami https://charts.bitnami.com/bitnami
helm install mysql bitnami/mysql -f deploy/mysql.yaml --namespace $NAMESPACE

# Wait for mysql to be ready
kubectl rollout status --watch --timeout=120s statefulset/mysql --namespace $NAMESPACE

# deploy automq-for-rocketmq
helm install automq-for-rocketmq ./charts/automq-for-rocketmq  -f deploy/helm_sample_values.yaml --set broker.image.repository=$ROCKETMQ_REPO --set broker.image.tag=$ROCKETMQ_VERSION --namespace $NAMESPACE

kubectl rollout status --watch --timeout=360s statefulset/automq-for-rocketmq-broker --namespace $NAMESPACE