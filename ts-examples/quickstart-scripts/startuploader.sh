#!/bin/bash

set -e

SCRIPT_DIR=$(dirname "$(readlink -f "$BASH_SOURCE")")
SOURCE_DIR=$(dirname $SCRIPT_DIR)

CUSTOM_OPTS="-DkafkaEnvironmentProviderClass=com.pinterest.kafka.tieredstorage.uploader.ExampleKafkaEnvironmentProvider -Dkafka.server.config=$SCRIPT_DIR/target/kafka_2.11-2.3.1/config/server.properties -Dstorage.service.endpoint.config.directory=$SCRIPT_DIR/config/storage"

java $CUSTOM_OPTS -cp $SOURCE_DIR/target/ts-examples-*.jar com.pinterest.kafka.tieredstorage.uploader.KafkaSegmentUploader $SCRIPT_DIR/config