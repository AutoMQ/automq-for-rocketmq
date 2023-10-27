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

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
REPO_DIR=$(dirname "$(dirname "$SCRIPT_DIR")")
export $REPO_DIR
cd "$SCRIPT_DIR" || exit 1
if [ ! -f "deploy/ddl.sql" ]
then
  cp "$REPO_DIR/controller/src/main/resources/ddl.sql" deploy/
fi

# make ddl to configmap
if [ ! -f "deploy/init-db-configmap.yaml" ]
then
  cp deploy/configmap-template.yaml deploy/init-db-configmap.yaml
  sed 's/^/    /' deploy/ddl.sql >> deploy/init-db-configmap.yaml
fi

kubectl apply -f deploy/init-db-configmap.yaml

echo "Deploying s3-localstack..."
# deploy s3-localstack
helm repo add localstack-charts https://localstack.github.io/helm-charts
helm install s3-localstack localstack-charts/localstack -f deploy/localstack_s3.yaml

sleep 10

echo "Deploying MySQL..."
# deploy mysql
helm repo add bitnami https://charts.bitnami.com/bitnami
helm install mysql bitnami/mysql -f deploy/mysql.yaml

sleep 10
echo "Deploying rocketmq..."
# deploy rocketmq-on-s3
helm install rocketmq-on-s3 . -f deploy/helm_sample_values.yaml


