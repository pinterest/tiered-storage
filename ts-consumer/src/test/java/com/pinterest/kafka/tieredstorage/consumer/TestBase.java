package com.pinterest.kafka.tieredstorage.consumer;

import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
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
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestBase {
    protected static final String TEMP_LOG_DIR = "/tmp/kafka-unit";
    @RegisterExtension
    protected static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource()
            .withBrokerProperty("log.dir", TEMP_LOG_DIR)
            .withBrokerProperty("log.segment.bytes", "5000")
            .withBrokerProperty("log.segment.delete.delay.ms", "1000");
    private static final Logger LOG = LogManager.getLogger(TestBase.class.getName());

    protected void sendTestData(String topic, int partition, int numRecords) {
        sharedKafkaTestResource.getKafkaTestUtils().produceRecords(numRecords, topic, partition);
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
