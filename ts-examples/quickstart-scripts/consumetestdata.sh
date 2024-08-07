#!/bin/bash

set -e

SCRIPT_DIR=$(dirname "$(readlink -f "$BASH_SOURCE")")
SOURCE_DIR=$(dirname $SCRIPT_DIR)

CUSTOM_OPTS="-Dstorage.service.endpoint.config.directory=$SCRIPT_DIR/config/storage"

java $CUSTOM_OPTS -cp $SOURCE_DIR/target/ts-examples-*.jar com.pinterest.kafka.tieredstorage.consumer.ExampleLocalTieredStorageConsumer -t my_test_topic -p 0