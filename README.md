<h1 align="center">
AutoMQ for Apache RocketMQ
</h1>
<h3 align="center">
    A cloud native implementation for Apache RocketMQ 5.0
</h3>

[![Build Status][maven-build-image]][maven-build-url]
[![Release][release-image]][release-url]
[![License][license-image]][license-url]

## What is AutoMQ for Apache RocketMQ

AutoMQ for Apache RocketMQ is redesigned based on cloud infrastructure, and users benefit from **elastic computing
resources** and nearly **unlimited cheap storage** in the cloud. AutoMQ for Apache RocketMQ guarantees **100% functional
compatibility** with Apache RocketMQ.

Compared to Apache RocketMQ, AutoMQ for Apache RocketMQ offers the following advantages:

- Enhanced Scalability: Separate storage to services rather than software, enabling seamless scaling to meet varying
  workloads. Maximize the use of Spot instances to leverage one of the biggest advantages of cloud computing:
  elasticity.

- Cost Efficiency: Using object storage as primary storage has made the storage more resilient and provided nearly
  unlimited capacity. Additionally, shared storage architecture is better suited for migration and can save up to 80% on
  your bill.

- Simplified Management: AutoMQ for Apache RocketMQ uses a reliable metadata service and cluster controller to
  automatically balance traffic between brokers and isolate abnormal nodes. This greatly simplifies the management of
  the cluster.

<img src="architecture.webp" style="width:800px;display:block;margin:0 auto;">

## Quick Start

Pre-requirements: docker and docker-compose

1. Clone the project via git: `git clone https://github.com/AutoMQ/automq-for-rocketmq.git && cd automq-for-rocketmq`
2. Run `./distribution/build.sh` to build the docker image.
3. Run `./distribution/compose.sh` to start the service, which includes 1 MySQL server and 2 RocketMQ brokers.
4. Start the producer and consumer to produce and consume messages,
   See [rocketmq-clients](https://github.com/apache/rocketmq-clients) for more details.

[maven-build-image]: https://github.com/AutoMQ/automq-for-rocketmq/actions/workflows/build-ci.yml/badge.svg

[maven-build-url]: https://github.com/AutoMQ/automq-for-rocketmq/actions/workflows/build-ci.yml

[release-image]: https://img.shields.io/badge/release-download-orange.svg

[release-url]: https://github.com/AutoMQ/automq-for-rocketmq/releases

[license-image]: https://img.shields.io/badge/license-Apache%202-4EB1BA.svg

[license-url]: https://www.apache.org/licenses/LICENSE-2.0.html
