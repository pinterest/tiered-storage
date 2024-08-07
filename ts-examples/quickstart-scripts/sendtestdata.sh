#!/bin/bash

set -e

SCRIPT_DIR=$(dirname "$(readlink -f "$BASH_SOURCE")")
SOURCE_DIR=$(dirname $SCRIPT_DIR)


java -cp $SOURCE_DIR/target/ts-examples-*.jar com.pinterest.kafka.tieredstorage.producer.ExampleLocalKafkaProducer -t my_test_topic -n 10000000