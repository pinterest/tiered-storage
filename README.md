# Kafka Tiered Storage
Kafka Tiered Storage is a framework that allows [Apache Kafka](https://kafka.apache.org/) brokers to offload finalized log segments to a remote storage system. 
This allows Kafka to maintain a smaller disk footprint and reduce the need for expensive storage on the brokers. 
The framework also provides a Kafka client compatible consumer that can consume from both the broker and the remote storage system.

Pinterest's implementation of Kafka Tiered Storage provides a Kafka broker independent approach to tiered storage. It consists of two main components:
1. [Uploader](ts-segment-uploader): A continuous process that runs on each Kafka broker and uploads finalized log segments to a remote storage system (e.g. Amazon S3, with unique prefix per Kafka cluster and topic).
2. [Consumer](ts-consumer): A Kafka client compatible consumer that consumes from both tiered storage log segments and Kafka cluster.

A third module [ts-common](ts-common) contains common classes and interfaces that are used by the `ts-consumer` and `ts-segment-uploader` modules, such as Metrics, StorageEndpointProvider, etc.

Feel free to read into each module's README for more details.

## Why Tiered Storage?
[Apache Kafka](https://kafka.apache.org/) is a distributed event streaming platform that stores partitioned and replicated log segments on disk for
a configurable retention period. However, as data volume and/or retention periods grow, the disk footprint of Kafka clusters can become expensive. 
Tiered Storage allows Kafka to offload finalized log segments to a more cost-effective remote storage system, reducing the need for expensive storage on the brokers.

With Tiered Storage, you can:
1. Maintain a smaller overall broker footprint, reducing operational costs
2. Retain data for longer periods of time while avoiding horizontal and vertical scaling of Kafka clusters
3. Reduce CPU, network, and disk I/O utilization on brokers by reading directly from remote storage

## Highlights
- **Kafka Broker Independent**: The tiered storage solution is designed to be Kafka broker independent, meaning it runs as an independent process alongside the Kafka server process. Currently, it only supports ZooKeeper-based Kafka versions. KRaft support is WIP.
- **Fault Tolerant**: Broker restarts, replacements, leadership changes, and other common Kafka operations / issues are handled gracefully.
- **Skip the broker entirely during consumption**: The consumer can read from both broker and Tiered Storage backend filesystem. When in `TIERED_STORAGE_ONLY` mode, the consumption loop does not touch the broker itself, allowing for reduction in broker resource utilization.
- **Pluggable Storage Backends**: The framework is designed to be backend-agnostic. Currently, only S3 is supported. More backend filesystems will be supported in the near future.
- **S3 Partitioning**: Prefix-entropy (salting) is configurable out-of-the-box to allow for prefix-partitioned S3 buckets, allowing for better scalability by avoiding request rate hotspots.
- **Metrics**: Comprehensive metrics are provided out-of-the-box for monitoring and alerting purposes.

# Quick Start
Detailed quickstart instructions are available [here](docs/quickstart.md).

# Usage
Using Kafka Tiered Storage consists of the following high-level steps:
1. Have a remote storage system ready to accept reads and writes of log segments (e.g. Amazon S3 bucket)
2. Configure and start [ts-segment-uploader](ts-segment-uploader) on each Kafka broker
3. Use [ts-consumer](ts-consumer) to read from either the broker or the remote storage system
4. Monitor and manage the tiered storage system using the provided metrics and tools

Feel free to read into each module's README for more details.

# Architecture
![Architecture](docs/images/architecture.png)

# Current Status
**Kafka Tiered Storage is currently under active development and the APIs may change over time.**

Kafka Tiered Storage currently supports the following remote storage systems:
- Amazon S3

Some of our planned features and improvements:

- KRaft support
- More storage system support (e.g. HDFS)
- Integration with [PubSub Client](https://github.com/pinterest/psc) (backend-agnostic client library)

Contributions are always welcome!

# Maintainers
- Vahid Hashemian
- Jeff Xiang

# License
Kafka Tiered Storage is distributed under Apache License, Version 2.0.
