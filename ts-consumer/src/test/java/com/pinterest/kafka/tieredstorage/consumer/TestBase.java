package com.pinterest.kafka.tieredstorage.consumer;

import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestBase {
    protected static final String TEMP_LOG_DIR = "/tmp/kafka-unit";
    @RegisterExtension
    protected static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource()
            .withBrokerProperty("log.dir", TEMP_LOG_DIR)
            .withBrokerProperty("log.segment.bytes", "5000")
            .withBrokerProperty("log.segment.delete.delay.ms", "1000")
            .withBrokerProperty("log.retention.check.interval.ms", "30000") // these are hard-coded to align with test scenarios
            .withBrokerProperty("log.retention.bytes", "250000");
    private static final Logger LOG = LogManager.getLogger(TestBase.class.getName());

    protected void sendTestData(String topic, int partition, int numRecords) {
        LOG.info(String.format("Going to produce %d records to topic %s, partition %d.", numRecords, topic, partition));
        sharedKafkaTestResource.getKafkaTestUtils().produceRecords(numRecords, topic, partition);
        LOG.info(String.format("Produced %d records to topic %s, partition %d.", numRecords, topic, partition));
    }
    @AfterEach
    void tearDown() throws IOException, ExecutionException, InterruptedException {
    }

    @AfterAll
    static void tearDownAll() throws IOException, InterruptedException {
        Thread.sleep(1000);
        deleteDirectory(Paths.get(TEMP_LOG_DIR));
        LOG.info("Deleted log.dir directory " + TEMP_LOG_DIR);
    }

    private static void deleteDirectory(Path directoryPath) throws IOException {
        if (Files.exists(directoryPath)) {
            Stream<Path> paths = Files.walk(directoryPath)
                    .sorted((path1, path2) -> -path1.compareTo(path2));
            try {
                for (Path path : paths.collect(Collectors.toList())) {
                    try {
                        Files.delete(path);
                    } catch (IOException e) {
                        LOG.warn("Failed to delete", e);
                    }
                }
            } catch (UncheckedIOException e) {
                // file was already deleted
            }
        }
    }

    private static long getMinOffsetForTopicPartition(String topic, int partition) throws IOException {
        Path tpPath = Paths.get(TEMP_LOG_DIR, topic + "-" + partition);
        List<Path> files = Files.list(tpPath)
                .filter(path -> path.getFileName().toString().endsWith(".log") || path.getFileName().toString().endsWith(".index") || path.getFileName().toString().endsWith(".timeindex") || path.getFileName().toString().endsWith(".snapshot"))
                .sorted().collect(Collectors.toList());
        if (files.isEmpty()) {
            LOG.info("No files found in " + tpPath);
            return Long.MIN_VALUE;
        }
        long minOffset = Long.parseLong(files.get(0).toFile().getName().split("\\.")[0]);
        LOG.info("Min offset now is " + minOffset + " for topic " + topic + " partition " + partition);
        return minOffset;
    }

//    protected static long waitForRetentionCleanupAndVerify(String topic, int partition, long minOffsetToDelete) throws IOException {
//        long minOffsetNow = getMinOffsetForTopicPartition(topic, partition);
//        long start = System.currentTimeMillis();
//        while (minOffsetNow < minOffsetToDelete) {
//            if (System.currentTimeMillis() - start > 60000) {
//                LOG.info("Timeout waiting for retention cleanup for topic " + topic + " partition " + partition);
//                break;
//            }
//            minOffsetNow = Math.max(minOffsetNow, getMinOffsetForTopicPartition(topic, partition));
//            try {
//                Thread.sleep(500);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
//        }
//        LOG.info("After waiting, min offset now is " + minOffsetNow + " for topic " + topic + " partition " + partition);
//        return minOffsetNow;
//    }

    protected static long waitForRetentionCleanupAndVerify(String topic, int partition, long minOffsetToDelete) {
        long beginningOffsets = getBeginningOffsets(topic, partition);
        while (beginningOffsets < minOffsetToDelete) {
            if (System.currentTimeMillis() - beginningOffsets > 60000) {
                LOG.info("Timeout waiting for retention cleanup for topic " + topic + " partition " + partition);
                break;
            }
            beginningOffsets = Math.max(beginningOffsets, getBeginningOffsets(topic, partition));
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        LOG.info("After waiting, beginning offsets now is " + beginningOffsets + " for topic " + topic + " partition " + partition);
        return beginningOffsets;
    }

    protected static long getBeginningOffsets(String topic, int partition) {
        KafkaConsumer<String, String> consumer = sharedKafkaTestResource.getKafkaTestUtils().getKafkaConsumer(StringDeserializer.class, StringDeserializer.class);
        long offset = consumer.beginningOffsets(Collections.singleton(new TopicPartition(topic, partition))).get(new TopicPartition(topic, partition));
        LOG.info("Beginning offset now is " + offset + " for topic " + topic + " partition " + partition);
        consumer.close();
        return offset;
    }

    protected static void createTopicAndVerify(SharedKafkaTestResource testResource, String topic, int partitions)
            throws InterruptedException {
        LOG.info(String.format("Going to create topic %s.", topic));
        testResource.getKafkaTestUtils().createTopic(topic, partitions, (short) 1);
        while (!testResource.getKafkaTestUtils().getTopicNames().contains(topic)) {
            Thread.sleep(1000);
        }
        LOG.info(String.format("Topic %s was created.", topic));
    }

    public static void deleteTopicAndVerify(SharedKafkaTestResource testResource, String topic)
            throws InterruptedException, ExecutionException {
        LOG.info(String.format("Going to delete topic %s.", topic));
        testResource.getKafkaTestUtils().getAdminClient().deleteTopics(Collections.singletonList(topic)).all().get();
        while (testResource.getKafkaTestUtils().getTopicNames().contains(topic)) {
            Thread.sleep(1000);
        }
        LOG.info(String.format("Topic %s was deleted.", topic));
    }}
