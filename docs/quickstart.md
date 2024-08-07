# Quick Start

## Prerequisites
- Java 11+
- Maven 3+
- AWS S3 bucket of your choice
- Linux machine with access to the S3 bucket
- At least ~10-20 GB of free disk space

Clone the repo and then build with Maven:
```
mvn clean package -DskipTests
```

Now, navigate to the `ts-examples` directory: `cd ts-examples`.

## 1. Start Local Kafka
First, let's start a local Kafka cluster. You can do this by running
`./quickstart-scripts/startlocalkafka.sh`. This will do the following:
1. Download Kafka 2.3.1 binaries
2. Start a local cluster with 1 local broker
3. Create a topic `my_test_topic` with a single partition

## 2. Run Local SegmentUploader
Now, let's run the uploader locally.

### Configurations

#### Storage Endpoint Configuration
Create a JSON file inside [ts-examples/quickstart-scripts/config/storage](../ts-examples/quickstart-scripts/config/storage) and name it `my_test_kafka_cluster.json`. Don't worry about accidentally committing this file - files in this directory are ignored by `.gitignore`. The content of the JSON file should look like:
```
{
    "bucket_public": "my-bucket-public",           ## change this to a bucket you have access to
    "bucket_pii": "my-bucket-pii",                 ## change this to a bucket you have access to
    "prefix": "kafka_tiered_storage_logs/prefix",  ## whatever prefix you'd like
    "topics": {
      "my_test_topic": {
        "pii": true
      }
    }
}
```
This JSON file specifies the upload S3 destination for each topic and is read by [ExampleS3StorageServiceEndpointProvider.java](../ts-examples/src/main/java/com/pinterest/kafka/tieredstorage/common/discovery/s3/ExampleS3StorageServiceEndpointProvider.java).

#### Uploader Configuration
The uploader configuration file has already been pre-filled for you [here](../ts-examples/quickstart-scripts/config/my_test_kafka_cluster.properties). Inspect it to see how the `storage.service.endpoint.provider.class` configuration takes in a FQDN classname for the `StorageServiceEndpointProvider` class that should be used by the uploader.

### Start Uploader
Simply execute `./quickstart-scripts/startuploader.sh` to start the uploader process.

## 3. Send Test Data
Let's send some test data to our local topic `my_test_topic`. Do this by running `./quickstart-scripts/sendtestdata.sh`, which will send 10,000,000 (10 million) messages to the topic every time you run the script.

### Track the log directory
Inspect `/tmp/kafka-logs/my_test_topic-0` directory and notice how there should be some log segments being generated. The one we're interested in ends in `.log` (likely `00000000000000000000.log` at the moment). Once that file fills up to 1G (default `log.segment.bytes`), Kafka will close that segment and open a new one. This is also the triggering condition for our uploader to upload the segment to S3.

Re-run the `./quickstart-scripts/sendtestdata.sh` script as many times as you want / need in order to see that the segment fill up to 1G and a new segment is created.

## 4. Verify Upload
Once the segment is rotated, the uploader should have triggered an upload of that `.log` segment file, along with the `.index`, `.timeindex` files. The uploader will also upload an `offset.wm` file after successful uploads of all 3. The uploader logs should show this if you take a look.

You can also verify the successful upload by inspecting the S3 path that you've defined in step 2 using `aws s3 ls s3://<my-bucket>/<my-prefix>/my_test_cluster/my_test_topic-0/`.

## 5. Consume Data
Finally, we can consume the data that we've uploaded. Do so by executing `./quickstart-scripts/consumetestdata.sh`. This runs a [TieredStorageConsumer](../ts-consumer/src/main/java/com/pinterest/kafka/tieredstorage/consumer/TieredStorageConsumer.java) instance using `TIERED_STORAGE_ONLY` consumption mode, meaning that it will only read data available on S3, and not from the Kafka broker directly.