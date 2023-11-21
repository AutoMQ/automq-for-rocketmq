# How to use

## Pre-requirement
- Kubernetes 1.18+;
- docker;
- Helm 3.3+;
- kubelet 1.20+;
- kubeblock 0.7+;
- kbcli 0.7+;
- mysql 5.7+;
- s3 storage;

## Launch automq-for-rocketmq cluster on KubeBlocks

Step 1: Invoke `helm install `to install automq-rocketmq chart
```shell
helm install automq-rocketmq -f helm/deploy/helm_sample_values.yaml ./kubeblocks/ 
```

Step 2: Invoke `kbcli cluster create` to create the automq-rocketmq cluster
```shell
kbcli cluster create automq-rocketmq --cluster-definition automq-rocketmq
```

## Tear automq-for-rocketmq cluster down
In current directory, `kbcli cluster stop automq-rocketmq`