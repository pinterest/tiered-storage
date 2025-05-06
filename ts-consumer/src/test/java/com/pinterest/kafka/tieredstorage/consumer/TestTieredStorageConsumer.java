package com.pinterest.kafka.tieredstorage.consumer;

import com.pinterest.kafka.tieredstorage.common.discovery.s3.MockS3StorageServiceEndpointProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class TestTieredStorageConsumer extends TestS3Base {

    private static final String TEST_CLUSTER_2 = "test-cluster-2";
    private static final String TEST_TOPIC_A = "test_topic_a";
    private TieredStorageConsumer<String, String> tsConsumer;

    @BeforeEach
    @Override
    void setup() throws InterruptedException, IOException, KeeperException, ExecutionException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        super.setup();
        S3Utils.overrideS3Client(s3Client);
        S3OffsetIndexHandler.overrideS3Client(s3Client);
        createTopicAndVerify(sharedKafkaTestResource, TEST_TOPIC_A, 3);
    }

    @AfterEach
    @Override
    void tearDown() throws ExecutionException, InterruptedException, IOException {
        deleteTopicAndVerify(sharedKafkaTestResource, TEST_TOPIC_A);
        super.tearDown();
        Thread.sleep(1000);
    }

    /**
     * Test that the consumer assigned to a single topic partition can consume records from Kafka only
     */
    @ParameterizedTest
    @EnumSource(TieredStorageConsumer.TieredStorageMode.class)
    void testSingleTopicPartitionAssignConsumptionNoTieredStorage(TieredStorageConsumer.TieredStorageMode mode) throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        if (mode == TieredStorageConsumer.TieredStorageMode.TIERED_STORAGE_ONLY) {
            LOG.info("Skipping testSingleTopicPartitionAssignConsumptionNoS3 for TIERED_STORAGE_ONLY mode");
            return;
        }
        tsConsumer = getTieredStorageConsumer(mode);
        TopicPartition tp = new TopicPartition(TEST_TOPIC_A, 0);
        tsConsumer.assign(Collections.singleton(tp));
        ConsumerRecords<String, String> records = tsConsumer.poll(Duration.ofMillis(100));
        assertEquals(0, records.count());

        sendTestData(TEST_TOPIC_A, 0, 100);

        List<ConsumerRecord<String, String>> consumed = new ArrayList<>();
        List<ConsumerRecord<String, String>> tpRecords = null;

        while (consumed.size() < 100) {
            records = tsConsumer.poll(Duration.ofMillis(100));
            records.forEach(consumed::add);
            if (!records.records(tp).isEmpty()) {
                tpRecords = records.records(tp);
            }
        }

        assertNotNull(tpRecords);
        assertEquals(99, tpRecords.get(tpRecords.size() - 1).offset());
        assertEquals(Collections.singletonMap(tp, 100L), tsConsumer.getPositions());

        records = tsConsumer.poll(Duration.ofMillis(100));
        assertEquals(0, records.count());
        assertEquals(Collections.singletonMap(tp, 100L), tsConsumer.getPositions());

        sendTestData(TEST_TOPIC_A, 0, 300);

        consumed.clear();

        while (consumed.size() < 300) {
            records = tsConsumer.poll(Duration.ofMillis(100));
            records.forEach(consumed::add);
            if (!records.records(tp).isEmpty()) {
                tpRecords = records.records(tp);
            }
        }

        assertEquals(300, consumed.size());
        assertEquals(0, tsConsumer.poll(Duration.ofMillis(100)).count());
        assertEquals(Collections.singletonMap(tp, 400L), tsConsumer.getPositions());
        assertEquals(399, tpRecords.get(tpRecords.size() - 1).offset());
        tsConsumer.close();
    }

    /**
     * Test subscribe consumption from Kafka only
     */
    @ParameterizedTest
    @EnumSource(TieredStorageConsumer.TieredStorageMode.class)
    void testSingleTopicSubscribeConsumptionNoTieredStorage(TieredStorageConsumer.TieredStorageMode mode) throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        if (mode == TieredStorageConsumer.TieredStorageMode.TIERED_STORAGE_ONLY) {
            LOG.info("Skipping testSingleTopicSubscribeConsumptionNoS3 for TIERED_STORAGE_ONLY mode");
            return;
        }
        sendTestData(TEST_TOPIC_A, 0, 100);
        sendTestData(TEST_TOPIC_A, 1, 100);
        sendTestData(TEST_TOPIC_A, 2, 100);

        tsConsumer = getTieredStorageConsumer(mode);
        tsConsumer.subscribe(Collections.singleton(TEST_TOPIC_A));

        List<ConsumerRecord<String, String>> consumed = new ArrayList<>();
        while (consumed.size() < 300) {
            ConsumerRecords<String, String> records = tsConsumer.poll(Duration.ofMillis(100));
            records.forEach(consumed::add);
        }

        assertEquals(0, tsConsumer.poll(Duration.ofMillis(100)).count());
        int p0Count = 0;
        int p1Count = 0;
        int p2Count = 0;
        for (ConsumerRecord<String, String> record : consumed) {
            if (record.partition() == 0)
                p0Count++;
            if (record.partition() == 1)
                p1Count++;
            if (record.partition() == 2)
                p2Count++;
        }
        assertEquals(100, p0Count);
        assertEquals(100, p1Count);
        assertEquals(100, p2Count);
        assertEquals(100L, tsConsumer.getPositions().get(new TopicPartition(TEST_TOPIC_A, 0)));
        assertEquals(100L, tsConsumer.getPositions().get(new TopicPartition(TEST_TOPIC_A, 1)));
        assertEquals(100L, tsConsumer.getPositions().get(new TopicPartition(TEST_TOPIC_A, 2)));

        sendTestData(TEST_TOPIC_A, 1, 1000);

        consumed.clear();
        while (consumed.size() < 1000) {
            ConsumerRecords<String, String> records = tsConsumer.poll(Duration.ofMillis(100));
            records.forEach(consumed::add);
        }

        assertEquals(0, tsConsumer.poll(Duration.ofMillis(100)).count());
        p0Count = 0;
        p1Count = 0;
        p2Count = 0;
        for (ConsumerRecord<String, String> record : consumed) {
            if (record.partition() == 0)
                p0Count++;
            if (record.partition() == 1)
                p1Count++;
            if (record.partition() == 2)
                p2Count++;
        }

        assertEquals(0, p0Count);
        assertEquals(1000, p1Count);
        assertEquals(0, p2Count);
        assertEquals(100L, tsConsumer.getPositions().get(new TopicPartition(TEST_TOPIC_A, 0)));
        assertEquals(1100L, tsConsumer.getPositions().get(new TopicPartition(TEST_TOPIC_A, 1)));
        assertEquals(100L, tsConsumer.getPositions().get(new TopicPartition(TEST_TOPIC_A, 2)));
        tsConsumer.close();
    }

    @ParameterizedTest
    @EnumSource(TieredStorageConsumer.TieredStorageMode.class)
    void testSubscribeConsumptionTieredStorage(TieredStorageConsumer.TieredStorageMode mode) throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, InterruptedException {
        if (mode == TieredStorageConsumer.TieredStorageMode.KAFKA_ONLY) {
            LOG.info("Skipping testSubscribeConsumptionTieredStorage for KAFKA_ONLY mode");
            return;
        }

        prepareS3Mocks();
        putObjects(TEST_CLUSTER_2, TEST_TOPIC_A, 0, "src/test/resources/log-files/test_topic_a-0");
        putObjects(TEST_CLUSTER_2, TEST_TOPIC_A, 1, "src/test/resources/log-files/test_topic_a-1");
        putObjects(TEST_CLUSTER_2, TEST_TOPIC_A, 2, "src/test/resources/log-files/test_topic_a-2");
        tsConsumer = getTieredStorageConsumer(TieredStorageConsumer.TieredStorageMode.TIERED_STORAGE_ONLY);

        tsConsumer.subscribe(Collections.singleton(TEST_TOPIC_A));
//        kafkaConsumer.seekToBeginning(Arrays.asList(
//                new TopicPartition(TEST_TOPIC_A, 0),
//                new TopicPartition(TEST_TOPIC_A, 1),
//                new TopicPartition(TEST_TOPIC_A, 2))
//        ); // TODO: seek to middle

        ConsumerRecords<String, String> records;
        int[] consumedByPartition = new int[3];
        int totalConsumed = 0;
        while (totalConsumed < TEST_TOPIC_A_P0_NUM_RECORDS + TEST_TOPIC_A_P1_NUM_RECORDS + TEST_TOPIC_A_P2_NUM_RECORDS) {
            records = tsConsumer.poll(Duration.ofMillis(100));
            if (records.isEmpty()) {
                LOG.info("Polled empty records");
                Thread.sleep(500);
                continue;
            }
            for (ConsumerRecord<String, String> record : records) {
                int partition = record.partition();
                String expectedRecordKey = String.valueOf(consumedByPartition[partition]);

                // known record values
                assertEquals(expectedRecordKey, record.key());
                assertEquals("val-" + expectedRecordKey, record.value());
                assertEquals("header1", record.headers().headers("header1").iterator().next().key());
                assertEquals("header1-val", new String(record.headers().headers("header1").iterator().next().value()));
                consumedByPartition[partition]++;
            }
            totalConsumed = consumedByPartition[0] + consumedByPartition[1] + consumedByPartition[2];
        }

        assertNoMoreRecords(Duration.ofSeconds(5));
        assertEquals(TEST_TOPIC_A_P0_NUM_RECORDS, consumedByPartition[0]);
        assertEquals(TEST_TOPIC_A_P1_NUM_RECORDS, consumedByPartition[1]);
        assertEquals(TEST_TOPIC_A_P2_NUM_RECORDS, consumedByPartition[2]);
        assertEquals(TEST_TOPIC_A_P0_NUM_RECORDS + TEST_TOPIC_A_P1_NUM_RECORDS + TEST_TOPIC_A_P2_NUM_RECORDS, totalConsumed);

        tsConsumer.close();
        closeS3Mocks();
    }

    /**
     * Testing that seek correctly sets the position for the partition
     */
    @ParameterizedTest
    @EnumSource(TieredStorageConsumer.TieredStorageMode.class)
    void testSeek(TieredStorageConsumer.TieredStorageMode mode) throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        if (mode == TieredStorageConsumer.TieredStorageMode.TIERED_STORAGE_ONLY) {
            LOG.info("Skipping testSeek for TIERED_STORAGE_ONLY mode");
            return;
        }
        tsConsumer = getTieredStorageConsumer(mode);
        Collection<TopicPartition> toAssign = new HashSet<>(Arrays.asList(new TopicPartition(TEST_TOPIC_A, 0), new TopicPartition(TEST_TOPIC_A, 2)));
        tsConsumer.assign(toAssign);
        sendTestData(TEST_TOPIC_A, 0, 100);
        sendTestData(TEST_TOPIC_A, 1, 100);
        sendTestData(TEST_TOPIC_A, 2, 100);

        tsConsumer.seek(new TopicPartition(TEST_TOPIC_A, 0), 20L);

        List<ConsumerRecord<String, String>> consumed = new ArrayList<>();
        while (consumed.size() < 180) {
            ConsumerRecords<String, String> records = tsConsumer.poll(Duration.ofMillis(100));
            records.forEach(consumed::add);
        }

        assertNoMoreRecords(Duration.ofSeconds(5));

        int p0Count = 0;
        int p1Count = 0;
        int p2Count = 0;
        for (ConsumerRecord<String, String> record : consumed) {
            if (record.partition() == 0)
                p0Count++;
            if (record.partition() == 1)
                p1Count++;
            if (record.partition() == 2)
                p2Count++;
        }
        assertEquals(80, p0Count);
        assertEquals(0, p1Count);
        assertEquals(100, p2Count);

        assertEquals(100L, tsConsumer.getPositions().get(new TopicPartition(TEST_TOPIC_A, 0)));
        assertFalse(tsConsumer.getPositions().containsKey(new TopicPartition(TEST_TOPIC_A, 1)));
        assertEquals(100L, tsConsumer.getPositions().get(new TopicPartition(TEST_TOPIC_A, 2)));

        tsConsumer.seek(new TopicPartition(TEST_TOPIC_A, 2), 20L);
        consumed.clear();
        while (consumed.size() < 80) {
            ConsumerRecords<String, String> records = tsConsumer.poll(Duration.ofMillis(100));
            records.forEach(consumed::add);
        }

        assertNoMoreRecords(Duration.ofSeconds(2));
        p0Count = 0;
        p1Count = 0;
        p2Count = 0;
        for (ConsumerRecord<String, String> record : consumed) {
            if (record.partition() == 0)
                p0Count++;
            if (record.partition() == 1)
                p1Count++;
            if (record.partition() == 2)
                p2Count++;
        }
        assertEquals(0, p0Count);
        assertEquals(0, p1Count);
        assertEquals(80, p2Count);

        assertEquals(100L, tsConsumer.getPositions().get(new TopicPartition(TEST_TOPIC_A, 0)));
        assertFalse(tsConsumer.getPositions().containsKey(new TopicPartition(TEST_TOPIC_A, 1)));
        assertEquals(100L, tsConsumer.getPositions().get(new TopicPartition(TEST_TOPIC_A, 2)));
        tsConsumer.close();
    }

    /**
     * Test that the consumer can consume records from S3 only
     * @throws IOException
     */
    @ParameterizedTest
    @EnumSource(TieredStorageConsumer.TieredStorageMode.class)
    void testTieredStorageConsumption(TieredStorageConsumer.TieredStorageMode mode) throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        if (mode == TieredStorageConsumer.TieredStorageMode.KAFKA_ONLY) {
            LOG.info("Skipping testTieredStorageConsumption for KAFKA_ONLY mode");
            return;
        }
        tsConsumer = getTieredStorageConsumer(TieredStorageConsumer.TieredStorageMode.TIERED_STORAGE_ONLY);
        prepareS3Mocks();
        putObjects(TEST_CLUSTER_2, TEST_TOPIC_A, 0, "src/test/resources/log-files/test_topic_a-0");
        tsConsumer.assign(Collections.singleton(new TopicPartition(TEST_TOPIC_A, 0)));
        KafkaConsumer outOfRangeKafkaConsumer = Mockito.mock(KafkaConsumer.class);
        when(outOfRangeKafkaConsumer.poll(any())).thenThrow(new OffsetOutOfRangeException(new HashMap() {{
            put(new TopicPartition(TEST_TOPIC_A, 0), 0L);
        }}));
        when(outOfRangeKafkaConsumer.assignment()).thenReturn(Collections.singleton(new TopicPartition(TEST_TOPIC_A, 0)));
        tsConsumer.setKafkaConsumer(outOfRangeKafkaConsumer);

        ConsumerRecords<String, String> records;
        int consumed = 0;
        while ((records = tsConsumer.poll(Duration.ofMillis(100))).count() > 0) {
            for (ConsumerRecord<String, String> record : records) {
                String currRecordNumString = String.valueOf(consumed);

                // known record values
                assertEquals(currRecordNumString, record.key());
                assertEquals("val-" + currRecordNumString, record.value());
                assertEquals("header1", record.headers().headers("header1").iterator().next().key());
                assertEquals("header1-val", new String(record.headers().headers("header1").iterator().next().value()));
                consumed++;
            }
        }
        assertEquals(TEST_TOPIC_A_P0_NUM_RECORDS, consumed);
        assertEquals(TEST_TOPIC_A_P0_NUM_RECORDS, tsConsumer.getPositions().get(new TopicPartition(TEST_TOPIC_A, 0)));
        tsConsumer.close();
        closeS3Mocks();
    }

    /**
     * Test that the consumer can consume records, first from Kafka, then from S3
     * @throws IOException
     */
    @Test
    void testKafkaToTieredStorageConsumption() throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        tsConsumer = getTieredStorageConsumer(TieredStorageConsumer.TieredStorageMode.KAFKA_PREFERRED);
        int numRecords = 5000;
        TopicPartition tp = new TopicPartition(TEST_TOPIC_A, 0);
        tsConsumer.assign(Collections.singleton(new TopicPartition(TEST_TOPIC_A, 0)));
        sendTestData(TEST_TOPIC_A, 0, numRecords);
        int consumed = 0;
        ConsumerRecords<String, String> records;
        while (!(records = tsConsumer.poll(Duration.ofMillis(100))).isEmpty() || consumed == 0) {
            consumed += records.count();
        }
        assertEquals(numRecords, consumed);
        assertEquals(numRecords, tsConsumer.getPositions().get(tp));

        // next consumption should be from s3

        prepareS3Mocks();
        putObjects(TEST_CLUSTER_2, TEST_TOPIC_A, 0, "src/test/resources/log-files/test_topic_a-0");
                KafkaConsumer outOfRangeKafkaConsumer = Mockito.mock(KafkaConsumer.class);
        when(outOfRangeKafkaConsumer.poll(any())).thenThrow(new OffsetOutOfRangeException(new HashMap() {{
            put(new TopicPartition(TEST_TOPIC_A, 0), 0L);
        }}));
        when(outOfRangeKafkaConsumer.assignment()).thenReturn(Collections.singleton(tp));
        tsConsumer.setKafkaConsumer(outOfRangeKafkaConsumer);

        while ((records = tsConsumer.poll(Duration.ofMillis(100))).count() > 0) {
            consumed += records.count();
        }
        assertEquals(TEST_TOPIC_A_P0_NUM_RECORDS, consumed);
        assertEquals(TEST_TOPIC_A_P0_NUM_RECORDS, tsConsumer.getPositions().get(tp));
        tsConsumer.close();
        closeS3Mocks();
    }

    /**
     * Test that the consumer can consume records, first from S3, then from Kafka
     * @throws IOException
     */
    @Test
    void testTieredStorageToKafkaConsumption() throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        tsConsumer = getTieredStorageConsumer(TieredStorageConsumer.TieredStorageMode.KAFKA_PREFERRED);
        int numRecords = 5000;
        TopicPartition tp = new TopicPartition(TEST_TOPIC_A, 0);
        tsConsumer.assign(Collections.singleton(new TopicPartition(TEST_TOPIC_A, 0)));
        sendTestData(TEST_TOPIC_A, 0, numRecords);
        int consumed = 0;
        ConsumerRecords<String, String> records;
        KafkaConsumer<String, String> actualKafkaConsumer = tsConsumer.getKafkaConsumer();

        prepareS3Mocks();
        putObjects(TEST_CLUSTER_2, TEST_TOPIC_A, 0, "src/test/resources/log-files/test_topic_a-0");
        KafkaConsumer outOfRangeKafkaConsumer = Mockito.mock(KafkaConsumer.class);
        when(outOfRangeKafkaConsumer.poll(any())).thenThrow(new OffsetOutOfRangeException(new HashMap() {{
            put(new TopicPartition(TEST_TOPIC_A, 0), 0L);
        }}));
        when(outOfRangeKafkaConsumer.assignment()).thenReturn(Collections.singleton(tp));
        tsConsumer.setKafkaConsumer(outOfRangeKafkaConsumer);

        while (consumed < numRecords * 0.2 && !(records = tsConsumer.poll(Duration.ofMillis(100))).isEmpty()) {
            consumed += records.count();
        }
        long pos = tsConsumer.position(tp);
        assertEquals(pos, consumed);

        // next consumption should be from Kafka
        tsConsumer.setKafkaConsumer(actualKafkaConsumer);

        int kafkaConsumed = 0;
        while (!(records = tsConsumer.poll(Duration.ofMillis(100))).isEmpty() || consumed == 0) {
            kafkaConsumed += records.count();
            consumed += records.count();
        }
        assertEquals(numRecords, consumed);
        assertEquals(numRecords - pos, kafkaConsumed);
        assertEquals(numRecords, tsConsumer.getPositions().get(tp));
        tsConsumer.close();
        closeS3Mocks();
    }

    /**
     * Test beginningOffsets when the offset is in S3
     */
    @Test
    void testBeginningOffsetsS3() throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        tsConsumer = getTieredStorageConsumer(TieredStorageConsumer.TieredStorageMode.KAFKA_PREFERRED);
        // put s3 objects starting at offset 1000
        putEmptyObjects(TEST_CLUSTER_2, TEST_TOPIC_A, 0, 1000, 1000, 1000);

        tsConsumer.setTieredStorageLocations(Collections.singleton(TEST_TOPIC_A));
        tsConsumer.getS3Consumer().assign(Collections.singleton(new TopicPartition(TEST_TOPIC_A, 0)));
        Map<TopicPartition, Long> offsets = tsConsumer.getS3Consumer().beginningOffsets(Collections.singleton(new TopicPartition(TEST_TOPIC_A, 0)));
        assertEquals(1000L, offsets.get(new TopicPartition(TEST_TOPIC_A, 0)));
        tsConsumer.close();
    }

    @Test
    void testKafkaOnlyAssignAndPosition() throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        tsConsumer = getTieredStorageConsumer(TieredStorageConsumer.TieredStorageMode.KAFKA_ONLY);
        TopicPartition tp = new TopicPartition(TEST_TOPIC_A, 0);
        tsConsumer.assign(Collections.singleton(tp));
        assertEquals(0L, tsConsumer.position(tp));
        tsConsumer.close();
    }

    private void assertNoMoreRecords(Duration checkTime) {
        long begin = System.currentTimeMillis();
        while (System.currentTimeMillis() - begin < checkTime.toMillis()) {
            assertEquals(0, tsConsumer.poll(Duration.ofMillis(100)).count());
        }
    }

    private static TieredStorageConsumer<String, String> getTieredStorageConsumer(TieredStorageConsumer.TieredStorageMode mode) throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        String bootstrapServers = sharedKafkaTestResource.getKafkaConnectString();

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "tiered-storage-client-id");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "20000");
        properties.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "209715200");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.toString());
        properties.setProperty(TieredStorageConsumerConfig.TIERED_STORAGE_MODE_CONFIG, mode.toString());
        properties.setProperty(TieredStorageConsumerConfig.KAFKA_CLUSTER_ID_CONFIG, TEST_CLUSTER_2);
        properties.setProperty(TieredStorageConsumerConfig.OFFSET_RESET_CONFIG, TieredStorageConsumer.OffsetReset.EARLIEST.toString());
        properties.setProperty(TieredStorageConsumerConfig.STORAGE_SERVICE_ENDPOINT_PROVIDER_CLASS_CONFIG, MockS3StorageServiceEndpointProvider.class.getName());
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return new TieredStorageConsumer<>(properties);
    }
}
