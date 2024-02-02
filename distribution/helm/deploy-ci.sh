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

# add dependency repo
helm repo add localstack-charts https://localstack.github.io/helm-charts
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo add automq http://charts.automq.com/
helm repo update


# deploy s3-localstack
helm install s3-localstack localstack-charts/localstack --version 0.6.5 -f deploy/localstack_s3.yaml --namespace $NAMESPACE

# Wait for s3-localstack to be ready
kubectl rollout status --watch --timeout=120s replicaset/s3-localstack --namespace $NAMESPACE

# deploy mysql
helm install mysql bitnami/mysql --version 9.14.1 -f deploy/mysql.yaml --namespace $NAMESPACE

# Wait for mysql to be ready
kubectl rollout status --watch --timeout=120s statefulset/mysql --namespace $NAMESPACE

# deploy automq-for-rocketmq
helm install automq-for-rocketmq automq/automq-for-rocketmq -f deploy/helm_sample_values.yaml --set broker.image.repository=$ROCKETMQ_REPO --set broker.image.tag=$ROCKETMQ_VERSION --namespace $NAMESPACE

# Wait for automq-for-rocketmq to be ready
kubectl rollout status --watch --timeout=360s statefulset/automq-for-rocketmq-rocketmq-broker --namespace $NAMESPACE