package com.pinterest.kafka.tieredstorage.consumer;

import com.pinterest.kafka.tieredstorage.common.discovery.s3.ExampleS3StorageServiceEndpointProvider;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ExampleLocalTieredStorageConsumer {
    private static final Logger LOG = LogManager.getLogger(ExampleLocalTieredStorageConsumer.class.getName());
    private static final String TOPIC_OPT = "t";
    private static final String PARTITION_OPT = "p";

    private static TieredStorageConsumer<String, String> tsConsumer;

    public static void main(String[] args) throws ParseException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, IOException, InterruptedException {

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {  
            LOG.info("Shutdown hook triggered. Cleaning up resources...");  
            cleanup();  
        }));  

        Options options = new Options();

        Option topicOpt = new Option(TOPIC_OPT, "the topic to consume from");
        topicOpt.setRequired(true);
        topicOpt.setArgs(1);
        options.addOption(topicOpt);

        Option partitionOpt = new Option(PARTITION_OPT, "the partition to consume from");
        partitionOpt.setRequired(true);
        partitionOpt.setArgs(1);
        options.addOption(partitionOpt);

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        String topic = cmd.getOptionValue(TOPIC_OPT);
        int partition = Integer.parseInt(cmd.getOptionValue(PARTITION_OPT));

        LOG.info("Starting ExampleLocalTieredStorageConsumer with topic=" + topic);

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "tiered-storage-client-id");
        properties.setProperty(TieredStorageConsumerConfig.TIERED_STORAGE_MODE_CONFIG, TieredStorageConsumer.TieredStorageMode.TIERED_STORAGE_ONLY.toString());
        properties.setProperty(TieredStorageConsumerConfig.KAFKA_CLUSTER_ID_CONFIG, "my_test_kafka_cluster");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.toString());
        properties.setProperty(TieredStorageConsumerConfig.STORAGE_SERVICE_ENDPOINT_PROVIDER_CLASS_CONFIG, ExampleS3StorageServiceEndpointProvider.class.getName());
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        tsConsumer = new TieredStorageConsumer<>(properties);

        TopicPartition tp = new TopicPartition(topic, partition);
        tsConsumer.assign(Collections.singleton(tp));
        tsConsumer.seekToBeginning(Collections.singleton(tp));

        try {
            while (true) {
                ConsumerRecords<String, String> records = tsConsumer.poll(Duration.ofMillis(100));
                if (records.count() == 0) {
                    LOG.info("No records polled");
                    Thread.sleep(1000);
                }
                records.forEach(r -> LOG.info(String.format("Record [topic=%s, partition=%s, offset=%s]: value = %s", r.topic(), r.partition(), r.offset(), new String(r.value()))));
            }
        } catch (InterruptException e) {
            LOG.info("Interrupt caught");
        }
    }

    private static void cleanup() {
        LOG.info("Cleanup...");
        if (tsConsumer != null) {
            tsConsumer.close();
        }
    }
    
}
