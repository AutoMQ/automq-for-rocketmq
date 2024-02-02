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
NAMESPACE=$1

if [[ -z $NAMESPACE ]]; then
  NAMESPACE="default"
  echo "info: NAMESPACE is empty, use default namespace: $NAMESPACE"
fi

helm uninstall s3-localstack --namespace $NAMESPACE
helm uninstall mysql --namespace $NAMESPACE
helm uninstall automq-for-rocketmq --namespace $NAMESPACE

kubectl delete cm mysql-initdb-config --namespace $NAMESPACE