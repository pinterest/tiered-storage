# Kafka Tiered Storage
Kafka Tiered Storage is a broker-independent framework that allows [Apache Kafka](https://kafka.apache.org/) brokers
to offload finalized log segments to a remote storage system. 
This allows Kafka to maintain a smaller disk footprint and reduce the need for expensive storage on the brokers. 
The framework also provides a Kafka client compatible consumer that can consume from both the broker and the remote storage system.

Pinterest's implementation of Kafka Tiered Storage provides a Kafka broker-independent approach to tiered storage.
See the [differences between broker-independent and native tiered storage](#broker-independent-vs-native-tiered-storage).

It consists of two main components:
1. [Uploader](ts-segment-uploader): A continuous process that runs on each Kafka broker and uploads finalized log segments to a remote storage system (e.g. Amazon S3, with unique prefix per Kafka cluster and topic).
2. [Consumer](ts-consumer): A Kafka client compatible consumer that consumes from both tiered storage log segments and Kafka cluster.

A third module [ts-common](ts-common) contains common classes and interfaces that are used by the `ts-consumer` and `ts-segment-uploader` modules, such as Metrics, StorageEndpointProvider, etc.

Feel free to read into each module's README for more details.

# Why Tiered Storage?
[Apache Kafka](https://kafka.apache.org/) is a distributed event streaming platform that stores partitioned and replicated log segments on disk for
a configurable retention period. However, as data volume and/or retention periods grow, the disk footprint of Kafka clusters can become expensive. 
Tiered Storage allows Kafka to offload finalized log segments to a more cost-effective remote storage system, reducing the need for expensive storage on the brokers.

With Tiered Storage, you can:
1. Maintain a smaller overall broker footprint, reducing operational costs
2. Retain data for longer periods of time while avoiding horizontal and vertical scaling of Kafka clusters
3. Reduce CPU, network, and disk I/O utilization on brokers by reading directly from remote storage

## Broker-Independent vs. Native Tiered Storage
[KIP-405](https://cwiki.apache.org/confluence/display/KAFKA/KIP-405%3A+Kafka+Tiered+Storage?uclick_id=11f222c6-967b-4935-98a9-cc88aafad7f5)
provides a native, open-source offering to Tiered Storage for Kafka and is available starting from Apache Kafka 3.6.0.
The native Tiered Storage implementation is broker-dependent, meaning that the broker process itself is responsible 
for offloading finalized log segments to remote storage, and the broker is always in the critical path of consumption.

**This implementation of Kafka Tiered Storage is broker-independent**, meaning that the tiered storage process runs as a separate process alongside the Kafka server process,
and the broker is not always in the critical path of consumption.
This allows for more flexibility in adopting tiered storage, and accommodates more unpredictable consumption patterns. 
Some of the key advantages of a broker-independent approach are:

1. **You don't need to upgrade brokers**: While the native offering requires upgrading brokers to a version that supports Tiered Storage, a broker-independent approach does not.
2. **You can skip the broker entirely during consumption**: When in `TIERED_STORAGE_ONLY` mode, the consumption loop does not touch the broker itself, allowing for more
unpredictable spikes in consumption patterns without affecting the broker.
3. **Support consumer backfills and replays without affecting broker CPU**: When the broker is out of the critical path of consumption,
consumer backfills and replays can be done without needing to keep additional CPU buffer on the brokers just to support those surges.
4. **Avoid cross-AZ transfer costs**: While the native approach adds a cross-AZ network cost factor for consumers that are not AZ-aware,
this broker-independent approach avoids that cost for all consumers when reading directly from remote storage.
5. **Faster adoption, iteration, and improvements**: A broker-independent Tiered Storage solution lets you adopt and upgrade Tiered Storage without
waiting for Kafka upgrades. Improvements, bug fixes, and new features are released independently of Kafka releases.

# Highlights
- **Kafka Broker Independent**: The tiered storage solution is designed to be Kafka broker-independent. [Here's why we think it's better](#broker-independent-vs-native-tiered-storage).
- **Fault Tolerant**: Broker restarts, replacements, leadership changes, and other common Kafka operations / issues are handled gracefully.
- **Skip the broker entirely during consumption**: The consumer can read from both broker and Tiered Storage backend filesystem. When in `TIERED_STORAGE_ONLY` mode, the consumption loop does not touch the broker itself, allowing for reduction in broker resource utilization.
- **Pluggable Storage Backends**: The framework is designed to be backend-agnostic.
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
