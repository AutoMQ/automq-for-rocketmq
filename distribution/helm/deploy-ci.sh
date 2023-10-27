#!/bin/bash
# echo $KUBE_CONFIG_DATA | base64 -d > ./config

# DB_SCRIPTS=$1

# deploy s3-localstack
helm repo add localstack-charts https://localstack.github.io/helm-charts
helm install s3-localstack localstack-charts/localstack -f deploy/localstack_s3.yaml  
#   --kubeconfig ./config

# deploy mysql
helm repo add bitnami https://charts.bitnami.com/bitnami

helm install mysql bitnami/mysql -f deploy/mysql.yaml
# deploy rocketmq-on-s3
helm install rocketmq-on-s3 ../rocketmq-k8s-helm/ -f deploy/helm_sample_values.yaml


