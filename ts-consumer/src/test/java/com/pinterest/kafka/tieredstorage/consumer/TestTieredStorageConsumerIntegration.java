package com.pinterest.kafka.tieredstorage.consumer;

import com.pinterest.kafka.tieredstorage.common.CommonTestUtils;
import com.pinterest.kafka.tieredstorage.common.discovery.s3.MockS3StorageServiceEndpointProvider;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
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

import static com.pinterest.kafka.tieredstorage.common.CommonTestUtils.writeExpectedRecordFormatTestData;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class TestTieredStorageConsumerIntegration extends TestS3Base {

    private static final String TEST_CLUSTER = "test-cluster";
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
        putObjects(TEST_CLUSTER, TEST_TOPIC_A, 0, "src/test/resources/log-files/test_topic_a-0");
        putObjects(TEST_CLUSTER, TEST_TOPIC_A, 1, "src/test/resources/log-files/test_topic_a-1");
        putObjects(TEST_CLUSTER, TEST_TOPIC_A, 2, "src/test/resources/log-files/test_topic_a-2");
        tsConsumer = getTieredStorageConsumer(TieredStorageConsumer.TieredStorageMode.TIERED_STORAGE_ONLY);

        tsConsumer.subscribe(Collections.singleton(TEST_TOPIC_A));

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
                CommonTestUtils.validateRecordContent(CommonTestUtils.RecordContentType.TIERED_STORAGE, record);
                consumedByPartition[partition]++;
            }
            totalConsumed = consumedByPartition[0] + consumedByPartition[1] + consumedByPartition[2];
        }

        assertNoMoreRecords(tsConsumer, Duration.ofSeconds(5));
        assertEquals(TEST_TOPIC_A_P0_NUM_RECORDS, consumedByPartition[0]);
        assertEquals(TEST_TOPIC_A_P1_NUM_RECORDS, consumedByPartition[1]);
        assertEquals(TEST_TOPIC_A_P2_NUM_RECORDS, consumedByPartition[2]);
        assertEquals(TEST_TOPIC_A_P0_NUM_RECORDS + TEST_TOPIC_A_P1_NUM_RECORDS + TEST_TOPIC_A_P2_NUM_RECORDS, totalConsumed);

        tsConsumer.close();
        closeS3Mocks();
    }

    @ParameterizedTest
    @EnumSource(TieredStorageConsumer.TieredStorageMode.class)
    void testPositionKafka(TieredStorageConsumer.TieredStorageMode mode) throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        if (mode == TieredStorageConsumer.TieredStorageMode.TIERED_STORAGE_ONLY) {
            LOG.info("Skipping testPositionKafka for TIERED_STORAGE_ONLY mode");
            return;
        }

        Properties props = getStandardTieredStorageConsumerProperties(mode, sharedKafkaTestResource.getKafkaConnectString());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.name().toLowerCase());
        tsConsumer = new TieredStorageConsumer<>(props);

        TopicPartition tpa0 = new TopicPartition(TEST_TOPIC_A, 0);
        TopicPartition tpa1 = new TopicPartition(TEST_TOPIC_A, 1);
        TopicPartition tpa2 = new TopicPartition(TEST_TOPIC_A, 2);
        Collection<TopicPartition> toAssign = new HashSet<>(Arrays.asList(tpa0, tpa2));
        tsConsumer.assign(toAssign);

        sendTestData(TEST_TOPIC_A, 0, 100);
        sendTestData(TEST_TOPIC_A, 1, 100);
        sendTestData(TEST_TOPIC_A, 2, 100);

        assertThrows(IllegalStateException.class, () -> tsConsumer.position(tpa1));

        int consumed = 0;
        while (consumed < 200) {
            ConsumerRecords<String, String> records = tsConsumer.poll(Duration.ofMillis(100));
            consumed += records.count();
        }

        assertNoMoreRecords(tsConsumer, Duration.ofSeconds(5));
        assertEquals(100L, tsConsumer.position(tpa0));
        assertThrows(IllegalStateException.class, () -> tsConsumer.position(tpa1));
        assertEquals(100L, tsConsumer.position(tpa2));

        tsConsumer.seek(tpa0, 20L);
        assertEquals(20L, tsConsumer.position(tpa0));
        assertThrows(IllegalStateException.class, () -> tsConsumer.position(tpa1));
        assertEquals(100L, tsConsumer.position(tpa2));

        tsConsumer.seek(tpa2, 30L);
        assertEquals(20L, tsConsumer.position(tpa0));
        assertThrows(IllegalStateException.class, () -> tsConsumer.position(tpa1));
        assertEquals(30L, tsConsumer.position(tpa2));

        List<ConsumerRecord<String, String>> consumedRecords = new ArrayList<>();
        while (consumedRecords.size() < 150) {
            ConsumerRecords<String, String> records = tsConsumer.poll(Duration.ofMillis(100));
            records.forEach(consumedRecords::add);
        }

        assertNoMoreRecords(tsConsumer, Duration.ofSeconds(5));
        int p0Count = 0;
        int p1Count = 0;
        int p2Count = 0;

        for (ConsumerRecord<String, String> record : consumedRecords) {
            if (record.partition() == 0)
                p0Count++;
            if (record.partition() == 1)
                p1Count++;
            if (record.partition() == 2)
                p2Count++;
        }
        assertEquals(80, p0Count);
        assertEquals(0, p1Count);
        assertEquals(70, p2Count);

        assertEquals(100L, tsConsumer.position(tpa0));
        assertThrows(IllegalStateException.class, () -> tsConsumer.position(tpa1));
        assertEquals(100L, tsConsumer.position(tpa2));

        // test case when TS position is different from Kafka position
        tsConsumer.getPositions().put(tpa0, 50L);
        tsConsumer.getPositions().put(tpa1, 40L);
        tsConsumer.getPositions().put(tpa2, 30L);

        assertEquals(100L, tsConsumer.position(tpa0));
        assertThrows(IllegalStateException.class, () -> tsConsumer.position(tpa1));
        assertEquals(100L, tsConsumer.position(tpa2));

        tsConsumer.close();

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
        assertEquals(20, tsConsumer.position(new TopicPartition(TEST_TOPIC_A, 0)));

        List<ConsumerRecord<String, String>> consumed = new ArrayList<>();
        while (consumed.size() < 180) {
            ConsumerRecords<String, String> records = tsConsumer.poll(Duration.ofMillis(100));
            records.forEach(r -> {
                consumed.add(r);
            });
        }

        assertNoMoreRecords(tsConsumer, Duration.ofSeconds(5));

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

        assertNoMoreRecords(tsConsumer, Duration.ofSeconds(2));
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
        putObjects(TEST_CLUSTER, TEST_TOPIC_A, 0, "src/test/resources/log-files/test_topic_a-0");
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
                CommonTestUtils.validateRecordContent(CommonTestUtils.RecordContentType.TIERED_STORAGE, record);
                consumed++;
            }
        }
        assertEquals(TEST_TOPIC_A_P0_NUM_RECORDS, consumed);
        assertEquals(TEST_TOPIC_A_P0_NUM_RECORDS, tsConsumer.getPositions().get(new TopicPartition(TEST_TOPIC_A, 0)));
        tsConsumer.close();
        closeS3Mocks();
    }

    /**
     * Test that the consumer can consume records, first from Kafka, then from S3 by mocking the KafkaConsumer to throw
     * OffsetOutOfRangeException
     * @throws IOException
     */
    @Test
    void testKafkaToTieredStorageConsumption() throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        tsConsumer = getTieredStorageConsumer(TieredStorageConsumer.TieredStorageMode.KAFKA_PREFERRED);
        int numRecordsFromKafka = 500;
        TopicPartition tp = new TopicPartition(TEST_TOPIC_A, 0);
        tsConsumer.assign(Collections.singleton(new TopicPartition(TEST_TOPIC_A, 0)));
        sendTestData(TEST_TOPIC_A, 0, numRecordsFromKafka);
        int consumed = 0;
        while (consumed < numRecordsFromKafka) {
            ConsumerRecords<String, String> records = tsConsumer.poll(Duration.ofMillis(100));
            consumed += records.count();
        }
        assertEquals(numRecordsFromKafka, consumed);
        assertEquals(numRecordsFromKafka, tsConsumer.getPositions().get(tp));

        // next consumption should be from s3

        prepareS3Mocks();
        putObjects(TEST_CLUSTER, TEST_TOPIC_A, 0, "src/test/resources/log-files/test_topic_a-0");
        KafkaConsumer outOfRangeKafkaConsumer = Mockito.mock(KafkaConsumer.class);
        when(outOfRangeKafkaConsumer.poll(any())).thenThrow(new OffsetOutOfRangeException(new HashMap() {{
            put(new TopicPartition(TEST_TOPIC_A, 0), 0L);
        }}));
        when(outOfRangeKafkaConsumer.assignment()).thenReturn(Collections.singleton(tp));
        tsConsumer.setKafkaConsumer(outOfRangeKafkaConsumer);

        while (consumed < TEST_TOPIC_A_P0_NUM_RECORDS) {
            ConsumerRecords<String, String> records = tsConsumer.poll(Duration.ofMillis(100));
            consumed += records.count();
        }
        assertEquals(TEST_TOPIC_A_P0_NUM_RECORDS, consumed);
        assertEquals(TEST_TOPIC_A_P0_NUM_RECORDS, tsConsumer.getPositions().get(tp));
        tsConsumer.close();
        closeS3Mocks();
    }

    /**
     * Test that the consumer can consume records, first from S3, then from Kafka by mocking the KafkaConsumer to throw
     * OffsetOutOfRangeException
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
        putObjects(TEST_CLUSTER, TEST_TOPIC_A, 0, "src/test/resources/log-files/test_topic_a-0");
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
     * Test that the consumer can consume records, first from S3, then from Kafka by having the broker's log cleaner
     * delete some portion of the log segments due to retention.
     *
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InvocationTargetException
     * @throws NoSuchMethodException
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws InterruptedException
     */
    @Test
    void testTieredStorageToKafkaConsumptionSingleAssignmentWithLogCleaner() throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, InterruptedException {
        prepareS3Mocks();
        tsConsumer = getTieredStorageConsumer(TieredStorageConsumer.TieredStorageMode.KAFKA_PREFERRED);
        TopicPartition tp = new TopicPartition(TEST_TOPIC_A, 0);
        tsConsumer.assign(Collections.singleton(tp));
        putObjects(TEST_CLUSTER, TEST_TOPIC_A, 0, "src/test/resources/log-files/test_topic_a-0");
        writeExpectedRecordFormatTestData(CommonTestUtils.RecordContentType.KAFKA, sharedKafkaTestResource, TEST_TOPIC_A, 0, TEST_TOPIC_A_P0_NUM_RECORDS);
        long minOffsetOnKafka = waitForRetentionCleanupAndVerify(TEST_TOPIC_A, 0, 2000);
        int consumed = 0;
        int kafkaConsumed = 0;
        int tieredStorageConsumed = 0;
        long overlapOffsets = 0;
        boolean previousRecordFromKafka = false;
        while (TEST_TOPIC_A_P0_NUM_RECORDS > consumed) {
            long positionBeforePoll = tsConsumer.position(tp);
            ConsumerRecords<String, String> records = tsConsumer.poll(Duration.ofMillis(100));
            // update minOffsetOnKafka if necessary
            minOffsetOnKafka = waitForRetentionCleanupAndVerify(TEST_TOPIC_A, 0, 2000);
            for (ConsumerRecord<String, String> record : records) {
                if (positionBeforePoll < minOffsetOnKafka) {
                    // expected from Tiered Storage
                    CommonTestUtils.validateRecordContent(CommonTestUtils.RecordContentType.TIERED_STORAGE, record);
                    tieredStorageConsumed++;
                } else {
                    if (!previousRecordFromKafka) {
                        // first record from Kafka
                        overlapOffsets = positionBeforePoll - minOffsetOnKafka;
                    }
                    // expected from Kafka
                    CommonTestUtils.validateRecordContent(CommonTestUtils.RecordContentType.KAFKA, record);
                    kafkaConsumed++;
                    previousRecordFromKafka = true;
                }
                consumed++;
            }
        }
        assertNoMoreRecords(tsConsumer, Duration.ofSeconds(5));
        assertEquals(TEST_TOPIC_A_P0_NUM_RECORDS, consumed);
        assertEquals(TEST_TOPIC_A_P0_NUM_RECORDS, tsConsumer.getPositions().get(tp));
        assertEquals(minOffsetOnKafka, tieredStorageConsumed - overlapOffsets);
        assertEquals(TEST_TOPIC_A_P0_NUM_RECORDS - minOffsetOnKafka - overlapOffsets, kafkaConsumed);
        assertEquals(TEST_TOPIC_A_P0_NUM_RECORDS, kafkaConsumed + tieredStorageConsumed);

        tsConsumer.close();
        closeS3Mocks();
    }

    /**
     * Test that the consumer can consume records, first from S3, then from Kafka by having the broker's log cleaner
     * delete some portion of the log segments due to retention. This is testing multi-partition assignment.
     *
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InvocationTargetException
     * @throws NoSuchMethodException
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws InterruptedException
     */
    @Test
    void testTieredStorageToKafkaConsumptionMultiAssignmentWithLogCleaner() throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, InterruptedException {
        prepareS3Mocks();
        tsConsumer = getTieredStorageConsumer(TieredStorageConsumer.TieredStorageMode.KAFKA_PREFERRED);
        TopicPartition tta0 = new TopicPartition(TEST_TOPIC_A, 0);
        TopicPartition tta1 = new TopicPartition(TEST_TOPIC_A, 1);
        TopicPartition tta2 = new TopicPartition(TEST_TOPIC_A, 2);
        tsConsumer.assign(Arrays.asList(tta0, tta1, tta2));
        putObjects(TEST_CLUSTER, TEST_TOPIC_A, 0, "src/test/resources/log-files/test_topic_a-0");
        putObjects(TEST_CLUSTER, TEST_TOPIC_A, 1, "src/test/resources/log-files/test_topic_a-1");
        putObjects(TEST_CLUSTER, TEST_TOPIC_A, 2, "src/test/resources/log-files/test_topic_a-2");
        writeExpectedRecordFormatTestData(CommonTestUtils.RecordContentType.KAFKA, sharedKafkaTestResource, TEST_TOPIC_A, 0, TEST_TOPIC_A_P0_NUM_RECORDS);
        writeExpectedRecordFormatTestData(CommonTestUtils.RecordContentType.KAFKA, sharedKafkaTestResource, TEST_TOPIC_A, 1, TEST_TOPIC_A_P1_NUM_RECORDS);
        writeExpectedRecordFormatTestData(CommonTestUtils.RecordContentType.KAFKA, sharedKafkaTestResource, TEST_TOPIC_A, 2, TEST_TOPIC_A_P2_NUM_RECORDS);
        int[] consumedByPartition = new int[3];
        int totalConsumed = 0;
        long[] positionsBeforePoll = new long[3];
        while (totalConsumed < TEST_TOPIC_A_P0_NUM_RECORDS + TEST_TOPIC_A_P1_NUM_RECORDS + TEST_TOPIC_A_P2_NUM_RECORDS) {
            positionsBeforePoll[0] = tsConsumer.position(tta0);
            positionsBeforePoll[1] = tsConsumer.position(tta1);
            positionsBeforePoll[2] = tsConsumer.position(tta2);
            // update minOffsetOnKafka if necessary
            long minOffsetOnKafka = waitForRetentionCleanupAndVerify(TEST_TOPIC_A, 0, 2000);
            long minOffsetOnKafka1 = waitForRetentionCleanupAndVerify(TEST_TOPIC_A, 1, 2000);
            long minOffsetOnKafka2 = waitForRetentionCleanupAndVerify(TEST_TOPIC_A, 2, 2000);
            long[] minOffsetsOnKafkaByPartition = new long[3];
            minOffsetsOnKafkaByPartition[0] = minOffsetOnKafka;
            minOffsetsOnKafkaByPartition[1] = minOffsetOnKafka1;
            minOffsetsOnKafkaByPartition[2] = minOffsetOnKafka2;
            ConsumerRecords<String, String> records = tsConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                if (positionsBeforePoll[record.partition()] < minOffsetsOnKafkaByPartition[record.partition()]) {
                    // expected from Tiered Storage
                    CommonTestUtils.validateRecordContent(CommonTestUtils.RecordContentType.TIERED_STORAGE, record);
                } else {
                    // expected from Kafka
                    CommonTestUtils.validateRecordContent(CommonTestUtils.RecordContentType.KAFKA, record);
                }
                consumedByPartition[record.partition()]++;
                totalConsumed++;
            }
        }

        assertNoMoreRecords(tsConsumer, Duration.ofSeconds(5));
        assertEquals(TEST_TOPIC_A_P0_NUM_RECORDS, consumedByPartition[0]);
        assertEquals(TEST_TOPIC_A_P1_NUM_RECORDS, consumedByPartition[1]);
        assertEquals(TEST_TOPIC_A_P2_NUM_RECORDS, consumedByPartition[2]);
        assertEquals(TEST_TOPIC_A_P0_NUM_RECORDS + TEST_TOPIC_A_P1_NUM_RECORDS + TEST_TOPIC_A_P2_NUM_RECORDS, totalConsumed);
        assertEquals(TEST_TOPIC_A_P0_NUM_RECORDS, tsConsumer.getPositions().get(tta0));
        assertEquals(TEST_TOPIC_A_P1_NUM_RECORDS, tsConsumer.getPositions().get(tta1));
        assertEquals(TEST_TOPIC_A_P2_NUM_RECORDS, tsConsumer.getPositions().get(tta2));

        tsConsumer.close();
        closeS3Mocks();
    }

    /**
     * Test that the consumer can consume records, first from S3, then from Kafka by having the broker's log cleaner
     * delete some portion of the log segments due to retention. This is testing subscription consumption.
     *
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InvocationTargetException
     * @throws NoSuchMethodException
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws InterruptedException
     */
    @Test
    void testTieredStorageToKafkaConsumptionSubscribeWithLogCleaner() throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, InterruptedException {
        prepareS3Mocks();
        tsConsumer = getTieredStorageConsumer(TieredStorageConsumer.TieredStorageMode.KAFKA_PREFERRED);
        TopicPartition tta0 = new TopicPartition(TEST_TOPIC_A, 0);
        TopicPartition tta1 = new TopicPartition(TEST_TOPIC_A, 1);
        TopicPartition tta2 = new TopicPartition(TEST_TOPIC_A, 2);
        tsConsumer.subscribe(Collections.singleton(TEST_TOPIC_A));
        putObjects(TEST_CLUSTER, TEST_TOPIC_A, 0, "src/test/resources/log-files/test_topic_a-0");
        putObjects(TEST_CLUSTER, TEST_TOPIC_A, 1, "src/test/resources/log-files/test_topic_a-1");
        putObjects(TEST_CLUSTER, TEST_TOPIC_A, 2, "src/test/resources/log-files/test_topic_a-2");
        writeExpectedRecordFormatTestData(CommonTestUtils.RecordContentType.KAFKA, sharedKafkaTestResource, TEST_TOPIC_A, 0, TEST_TOPIC_A_P0_NUM_RECORDS);
        writeExpectedRecordFormatTestData(CommonTestUtils.RecordContentType.KAFKA, sharedKafkaTestResource, TEST_TOPIC_A, 1, TEST_TOPIC_A_P1_NUM_RECORDS);
        writeExpectedRecordFormatTestData(CommonTestUtils.RecordContentType.KAFKA, sharedKafkaTestResource, TEST_TOPIC_A, 2, TEST_TOPIC_A_P2_NUM_RECORDS);
        int[] consumedByPartition = new int[3];
        int totalConsumed = 0;
        long[] positionsBeforePoll = new long[3];
        while (totalConsumed < TEST_TOPIC_A_P0_NUM_RECORDS + TEST_TOPIC_A_P1_NUM_RECORDS + TEST_TOPIC_A_P2_NUM_RECORDS) {
            if (totalConsumed > 0) {
                positionsBeforePoll[0] = tsConsumer.position(tta0);
                positionsBeforePoll[1] = tsConsumer.position(tta1);
                positionsBeforePoll[2] = tsConsumer.position(tta2);
            }
            // update minOffsetOnKafka if necessary
            long minOffsetOnKafka = waitForRetentionCleanupAndVerify(TEST_TOPIC_A, 0, 2000);
            long minOffsetOnKafka1 = waitForRetentionCleanupAndVerify(TEST_TOPIC_A, 1, 2000);
            long minOffsetOnKafka2 = waitForRetentionCleanupAndVerify(TEST_TOPIC_A, 2, 2000);
            long[] minOffsetsOnKafkaByPartition = new long[3];
            minOffsetsOnKafkaByPartition[0] = minOffsetOnKafka;
            minOffsetsOnKafkaByPartition[1] = minOffsetOnKafka1;
            minOffsetsOnKafkaByPartition[2] = minOffsetOnKafka2;
            ConsumerRecords<String, String> records = tsConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                if (positionsBeforePoll[record.partition()] < minOffsetsOnKafkaByPartition[record.partition()]) {
                    // expected from Tiered Storage
                    CommonTestUtils.validateRecordContent(CommonTestUtils.RecordContentType.TIERED_STORAGE, record);
                } else {
                    // expected from Kafka
                    CommonTestUtils.validateRecordContent(CommonTestUtils.RecordContentType.KAFKA, record);
                }
                consumedByPartition[record.partition()]++;
                totalConsumed++;
            }
        }

        assertNoMoreRecords(tsConsumer, Duration.ofSeconds(5));
        assertEquals(TEST_TOPIC_A_P0_NUM_RECORDS, consumedByPartition[0]);
        assertEquals(TEST_TOPIC_A_P1_NUM_RECORDS, consumedByPartition[1]);
        assertEquals(TEST_TOPIC_A_P2_NUM_RECORDS, consumedByPartition[2]);
        assertEquals(TEST_TOPIC_A_P0_NUM_RECORDS + TEST_TOPIC_A_P1_NUM_RECORDS + TEST_TOPIC_A_P2_NUM_RECORDS, totalConsumed);
        assertEquals(TEST_TOPIC_A_P0_NUM_RECORDS, tsConsumer.getPositions().get(tta0));
        assertEquals(TEST_TOPIC_A_P1_NUM_RECORDS, tsConsumer.getPositions().get(tta1));
        assertEquals(TEST_TOPIC_A_P2_NUM_RECORDS, tsConsumer.getPositions().get(tta2));

        tsConsumer.close();
        closeS3Mocks();
    }

    /**
     * Test beginningOffsets when the beginning offset is in Tiered Storage across all modes. In KAFKA_ONLY mode, the beginning
     * offsets should be the minimum offset on Kafka. Otherwise, in all other modes, the beginning offsets should be the
     * offsets in Tiered Storage if it exists. If not, it should be the minimum offset on Kafka.
     */
    // TODO: make this into a mocked unit test
    @ParameterizedTest
    @EnumSource(TieredStorageConsumer.TieredStorageMode.class)
    void testBeginningOffsetsTieredStorage(TieredStorageConsumer.TieredStorageMode mode) throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, InterruptedException {
        if (mode == TieredStorageConsumer.TieredStorageMode.KAFKA_ONLY) {
            LOG.info("Skipping testBeginningOffsetsTieredStorage for KAFKA_ONLY mode");
            return;
        }
        tsConsumer = getTieredStorageConsumer(mode);

        putEmptyObjects(TEST_CLUSTER, TEST_TOPIC_A, 0, 100, 2000, 100);
        putEmptyObjects(TEST_CLUSTER, TEST_TOPIC_A, 1, 150, 2000, 100);
        putEmptyObjects(TEST_CLUSTER, TEST_TOPIC_A, 2, 200, 2000, 100);

        // broker offsets should be cleaned up automatically by log cleaner
        writeExpectedRecordFormatTestData(CommonTestUtils.RecordContentType.KAFKA, sharedKafkaTestResource, TEST_TOPIC_A, 0, 6000);
        writeExpectedRecordFormatTestData(CommonTestUtils.RecordContentType.KAFKA, sharedKafkaTestResource, TEST_TOPIC_A, 1, 6000);
        writeExpectedRecordFormatTestData(CommonTestUtils.RecordContentType.KAFKA, sharedKafkaTestResource, TEST_TOPIC_A, 2, 6000);

        waitForRetentionCleanupAndVerify(TEST_TOPIC_A, 0, 101);
        waitForRetentionCleanupAndVerify(TEST_TOPIC_A, 1, 151);
        waitForRetentionCleanupAndVerify(TEST_TOPIC_A, 2, 201);

        TopicPartition tp0 = new TopicPartition(TEST_TOPIC_A, 0);
        TopicPartition tp1 = new TopicPartition(TEST_TOPIC_A, 1);
        TopicPartition tp2 = new TopicPartition(TEST_TOPIC_A, 2);
        Map<TopicPartition, Long> beginningOffsets = tsConsumer.beginningOffsets(Arrays.asList(tp0, tp1, tp2));

        assertEquals(100, beginningOffsets.get(tp0));
        assertEquals(150, beginningOffsets.get(tp1));
        assertEquals(200, beginningOffsets.get(tp2));

        tsConsumer.close();
    }

    /**
     * Test beginningOffsets when the beginning offset is in Kafka across all modes. This scenario is rare because
     * the beginning offsets are usually in Tiered Storage.
     */
    @ParameterizedTest
    @EnumSource(TieredStorageConsumer.TieredStorageMode.class)
    void testBeginningOffsetsKafka(TieredStorageConsumer.TieredStorageMode mode) throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, InterruptedException {
        tsConsumer = getTieredStorageConsumer(mode);

        putEmptyObjects(TEST_CLUSTER, TEST_TOPIC_A, 0, 100, 1000, 100);
        putEmptyObjects(TEST_CLUSTER, TEST_TOPIC_A, 1, 200, 1000, 100);
        putEmptyObjects(TEST_CLUSTER, TEST_TOPIC_A, 2, 300, 1000, 100);

        writeExpectedRecordFormatTestData(CommonTestUtils.RecordContentType.KAFKA, sharedKafkaTestResource, TEST_TOPIC_A, 0, 1000);
        writeExpectedRecordFormatTestData(CommonTestUtils.RecordContentType.KAFKA, sharedKafkaTestResource, TEST_TOPIC_A, 1, 1000);
        writeExpectedRecordFormatTestData(CommonTestUtils.RecordContentType.KAFKA, sharedKafkaTestResource, TEST_TOPIC_A, 2, 1000);

        TopicPartition tp0 = new TopicPartition(TEST_TOPIC_A, 0);
        TopicPartition tp1 = new TopicPartition(TEST_TOPIC_A, 1);
        TopicPartition tp2 = new TopicPartition(TEST_TOPIC_A, 2);
        Map<TopicPartition, Long> beginningOffsets = tsConsumer.beginningOffsets(Arrays.asList(tp0, tp1, tp2));

        if (mode == TieredStorageConsumer.TieredStorageMode.TIERED_STORAGE_ONLY) {
            assertEquals(100, beginningOffsets.get(tp0));
            assertEquals(200, beginningOffsets.get(tp1));
            assertEquals(300, beginningOffsets.get(tp2));
        } else {
            // KAFKA_ONLY and KAFKA_PREFERRED
            assertEquals(0, beginningOffsets.get(tp0));
            assertEquals(0, beginningOffsets.get(tp1));
            assertEquals(0, beginningOffsets.get(tp2));
        }

        tsConsumer.close();
    }

    @ParameterizedTest
    @EnumSource(TieredStorageConsumer.TieredStorageMode.class)
    void testOffsetResetPositionScenarios(TieredStorageConsumer.TieredStorageMode mode) throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        Properties props = getStandardTieredStorageConsumerProperties(mode, sharedKafkaTestResource.getKafkaConnectString());
        sendTestData(TEST_TOPIC_A, 0, 100);

        // test none for both Kafka and TS config
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        props.setProperty(TieredStorageConsumerConfig.OFFSET_RESET_CONFIG, TieredStorageConsumer.OffsetReset.NONE.name());
        tsConsumer = new TieredStorageConsumer<>(props);
        tsConsumer.assign(Collections.singleton(new TopicPartition(TEST_TOPIC_A, 0)));
        assertThrows(NoOffsetForPartitionException.class, () -> tsConsumer.position(new TopicPartition(TEST_TOPIC_A, 0)));
        tsConsumer.close();

        // test none for Kafka and earliest for TS config
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        props.setProperty(TieredStorageConsumerConfig.OFFSET_RESET_CONFIG, TieredStorageConsumer.OffsetReset.EARLIEST.name());
        tsConsumer = new TieredStorageConsumer<>(props);
        tsConsumer.assign(Collections.singleton(new TopicPartition(TEST_TOPIC_A, 0)));
        if (mode == TieredStorageConsumer.TieredStorageMode.KAFKA_ONLY) {
            assertThrows(NoOffsetForPartitionException.class, () -> tsConsumer.position(new TopicPartition(TEST_TOPIC_A, 0)));
        } else {
            assertEquals(0L, tsConsumer.position(new TopicPartition(TEST_TOPIC_A, 0)));
        }
        tsConsumer.close();

        // test earliest for both Kafka and TS config
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(TieredStorageConsumerConfig.OFFSET_RESET_CONFIG, TieredStorageConsumer.OffsetReset.EARLIEST.name());
        tsConsumer = new TieredStorageConsumer<>(props);
        tsConsumer.assign(Collections.singleton(new TopicPartition(TEST_TOPIC_A, 0)));
        assertEquals(0L, tsConsumer.position(new TopicPartition(TEST_TOPIC_A, 0)));
        tsConsumer.close();

        // test earliest for Kafka and none for TS config
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(TieredStorageConsumerConfig.OFFSET_RESET_CONFIG, TieredStorageConsumer.OffsetReset.NONE.name());
        tsConsumer = new TieredStorageConsumer<>(props);
        tsConsumer.assign(Collections.singleton(new TopicPartition(TEST_TOPIC_A, 0)));
        if (mode == TieredStorageConsumer.TieredStorageMode.KAFKA_ONLY) {
            assertEquals(0L, tsConsumer.position(new TopicPartition(TEST_TOPIC_A, 0)));
        } else {
            assertThrows(NoOffsetForPartitionException.class, () -> tsConsumer.position(new TopicPartition(TEST_TOPIC_A, 0)));
        }
        tsConsumer.close();

        // test latest for both Kafka and TS config
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.setProperty(TieredStorageConsumerConfig.OFFSET_RESET_CONFIG, TieredStorageConsumer.OffsetReset.LATEST.name());
        tsConsumer = new TieredStorageConsumer<>(props);
        tsConsumer.assign(Collections.singleton(new TopicPartition(TEST_TOPIC_A, 0)));
        assertEquals(100L, tsConsumer.position(new TopicPartition(TEST_TOPIC_A, 0)));
        tsConsumer.close();

        // test latest for Kafka and earliest for TS config
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.setProperty(TieredStorageConsumerConfig.OFFSET_RESET_CONFIG, TieredStorageConsumer.OffsetReset.EARLIEST.name());
        tsConsumer = new TieredStorageConsumer<>(props);
        tsConsumer.assign(Collections.singleton(new TopicPartition(TEST_TOPIC_A, 0)));
        if (mode == TieredStorageConsumer.TieredStorageMode.KAFKA_ONLY) {
            assertEquals(100L, tsConsumer.position(new TopicPartition(TEST_TOPIC_A, 0)));
        } else {
            assertEquals(0L, tsConsumer.position(new TopicPartition(TEST_TOPIC_A, 0)));
        }
        tsConsumer.close();

        // test latest for Kafka and none for TS config
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.setProperty(TieredStorageConsumerConfig.OFFSET_RESET_CONFIG, TieredStorageConsumer.OffsetReset.NONE.name());
        tsConsumer = new TieredStorageConsumer<>(props);
        tsConsumer.assign(Collections.singleton(new TopicPartition(TEST_TOPIC_A, 0)));
        if (mode == TieredStorageConsumer.TieredStorageMode.KAFKA_ONLY) {
            assertEquals(100L, tsConsumer.position(new TopicPartition(TEST_TOPIC_A, 0)));
        } else {
            assertThrows(NoOffsetForPartitionException.class, () -> tsConsumer.position(new TopicPartition(TEST_TOPIC_A, 0)));
        }
        tsConsumer.close();

        // test earliest for Kafka and latest for TS config
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(TieredStorageConsumerConfig.OFFSET_RESET_CONFIG, TieredStorageConsumer.OffsetReset.LATEST.name());
        tsConsumer = new TieredStorageConsumer<>(props);
        tsConsumer.assign(Collections.singleton(new TopicPartition(TEST_TOPIC_A, 0)));
        if (mode == TieredStorageConsumer.TieredStorageMode.KAFKA_ONLY) {
            assertEquals(0L, tsConsumer.position(new TopicPartition(TEST_TOPIC_A, 0)));
        } else {
            assertEquals(100L, tsConsumer.position(new TopicPartition(TEST_TOPIC_A, 0)));
        }
        tsConsumer.close();
    }

    /**
     * Tests that the consumer can be assigned a topic partition and the position can be retrieved when using KAFKA_ONLY mode.
     *
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InvocationTargetException
     * @throws NoSuchMethodException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    @ParameterizedTest
    @EnumSource(TieredStorageConsumer.TieredStorageMode.class)
    void testAssignAndPosition(TieredStorageConsumer.TieredStorageMode mode) throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        tsConsumer = getTieredStorageConsumer(mode);
        TopicPartition tp0 = new TopicPartition(TEST_TOPIC_A, 0);
        TopicPartition tp1 = new TopicPartition(TEST_TOPIC_A, 1);
        TopicPartition tp2 = new TopicPartition(TEST_TOPIC_A, 2);
        tsConsumer.assign(Arrays.asList(tp0, tp1, tp2));
        assertEquals(0L, tsConsumer.position(tp0));
        assertEquals(0L, tsConsumer.position(tp1));
        assertEquals(0L, tsConsumer.position(tp2));
        tsConsumer.close();
    }

    @ParameterizedTest
    @EnumSource(TieredStorageConsumer.TieredStorageMode.class)
    void testSubscribeAndPositionThrowsException(TieredStorageConsumer.TieredStorageMode mode) throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        tsConsumer = getTieredStorageConsumer(mode);
        tsConsumer.subscribe(Collections.singleton(TEST_TOPIC_A));
        assertThrows(IllegalStateException.class, () -> tsConsumer.position(new TopicPartition(TEST_TOPIC_A, 0)));
        tsConsumer.close();
    }

    @ParameterizedTest
    @EnumSource(TieredStorageConsumer.TieredStorageMode.class)
    @Timeout(30)
    void testSubscribeAndPositionAfterAssignment(TieredStorageConsumer.TieredStorageMode mode) throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        tsConsumer = getTieredStorageConsumer(mode);
        tsConsumer.subscribe(Collections.singleton(TEST_TOPIC_A));
        assertEquals(0, tsConsumer.assignment().size());
        if (mode != TieredStorageConsumer.TieredStorageMode.KAFKA_ONLY) {
            // if not kafka only, partitions should be assigned after first poll due to waitForAssignment loop
            tsConsumer.poll(Duration.ofMillis(100));
        } else {
            // if kafka only, partitions should be assigned after loop
            while (tsConsumer.assignment().isEmpty()) {
                tsConsumer.poll(Duration.ofMillis(100));
            }
        }
        assertEquals(3, tsConsumer.assignment().size());

        TopicPartition tpa0 = new TopicPartition(TEST_TOPIC_A, 0);
        TopicPartition tpa1 = new TopicPartition(TEST_TOPIC_A, 1);
        TopicPartition tpa2 = new TopicPartition(TEST_TOPIC_A, 2);

        assertTrue(tsConsumer.assignment().contains(tpa0));
        assertTrue(tsConsumer.assignment().contains(tpa1));
        assertTrue(tsConsumer.assignment().contains(tpa2));

        assertEquals(0L, tsConsumer.position(tpa0));
        assertEquals(0L, tsConsumer.position(tpa1));
        assertEquals(0L, tsConsumer.position(tpa2));

        tsConsumer.close();
    }

    @ParameterizedTest
    @EnumSource(TieredStorageConsumer.TieredStorageMode.class)
    void testCommitSyncSingleTopicSubscribeTieredStorage(TieredStorageConsumer.TieredStorageMode mode) throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, InterruptedException {
        if (mode == TieredStorageConsumer.TieredStorageMode.KAFKA_ONLY) {
            LOG.info("Skipping testCommitSyncSingleTopicSubscribeTieredStorage for KAFKA_ONLY mode");
            return;
        }

        prepareS3Mocks();

        Properties props = getStandardTieredStorageConsumerProperties(mode, sharedKafkaTestResource.getKafkaConnectString());
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
        tsConsumer = new TieredStorageConsumer<>(props);
        tsConsumer.subscribe(Collections.singleton(TEST_TOPIC_A));

        TopicPartition tta0 = new TopicPartition(TEST_TOPIC_A, 0);
        TopicPartition tta1 = new TopicPartition(TEST_TOPIC_A, 1);
        TopicPartition tta2 = new TopicPartition(TEST_TOPIC_A, 2);

        putObjects(TEST_CLUSTER, TEST_TOPIC_A, 0, "src/test/resources/log-files/test_topic_a-0");
        putObjects(TEST_CLUSTER, TEST_TOPIC_A, 1, "src/test/resources/log-files/test_topic_a-1");
        putObjects(TEST_CLUSTER, TEST_TOPIC_A, 2, "src/test/resources/log-files/test_topic_a-2");

        int[] consumedByPartition = new int[3];
        int totalConsumed = 0;
        int totalRecords = TEST_TOPIC_A_P0_NUM_RECORDS + TEST_TOPIC_A_P1_NUM_RECORDS + TEST_TOPIC_A_P2_NUM_RECORDS;

        while (totalConsumed < totalRecords / 3) {
            ConsumerRecords<String, String> records = tsConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                consumedByPartition[record.partition()]++;
                totalConsumed++;
            }
            tsConsumer.commitSync();
            assertEquals(consumedByPartition[0], tsConsumer.committed(tta0).offset());
            assertEquals(consumedByPartition[1], tsConsumer.committed(tta1).offset());
            assertEquals(consumedByPartition[2], tsConsumer.committed(tta2).offset());
        }
        long tta0Committed = tsConsumer.committed(tta0).offset();
        long tta1Committed = tsConsumer.committed(tta1).offset();
        long tta2Committed = tsConsumer.committed(tta2).offset();
        long[] committedByPartition = new long[]{tta0Committed, tta1Committed, tta2Committed};

        assertEquals(consumedByPartition[0], tta0Committed);
        assertEquals(consumedByPartition[1], tta1Committed);
        assertEquals(consumedByPartition[2], tta2Committed);

        tsConsumer.close();     // simulate close and restart from committed offsets

        tsConsumer = getTieredStorageConsumer(mode);
        tsConsumer.subscribe(Collections.singleton(TEST_TOPIC_A));

        boolean[] partitionRecordReceived = new boolean[3];
        while (totalConsumed < totalRecords) {
            ConsumerRecords<String, String> records = tsConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record: records) {
                if (!partitionRecordReceived[record.partition()]) {
                    partitionRecordReceived[record.partition()] = true;
                    assertEquals(committedByPartition[record.partition()], record.offset());
                }
                consumedByPartition[record.partition()]++;
                totalConsumed++;
            }
            tsConsumer.commitSync();
            assertEquals(consumedByPartition[0], tsConsumer.committed(tta0).offset());
            assertEquals(consumedByPartition[1], tsConsumer.committed(tta1).offset());
            assertEquals(consumedByPartition[2], tsConsumer.committed(tta2).offset());
        }

        assertNoMoreRecords(tsConsumer, Duration.ofSeconds(5));
        Map<TopicPartition, OffsetAndMetadata> committed = tsConsumer.committed(new HashSet<>(Arrays.asList(tta0, tta1, tta2)));
        for (TopicPartition tp : committed.keySet()) {
            assertEquals(consumedByPartition[tp.partition()], committed.get(tp).offset());
        }

        tsConsumer.close();
        closeS3Mocks();
    }

    @ParameterizedTest
    @EnumSource(TieredStorageConsumer.TieredStorageMode.class)
    void testCommitSyncSingleTopicSubscribeKafka(TieredStorageConsumer.TieredStorageMode mode) throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        if (mode == TieredStorageConsumer.TieredStorageMode.TIERED_STORAGE_ONLY) {
            LOG.info("Skipping testCommitSyncSingleTopicSubscribeKafka for TIERED_STORAGE_ONLY mode");
            return;
        }
        String groupId = "test-consumer-group-" + mode.toString().toLowerCase();
        Properties props = getStandardTieredStorageConsumerProperties(mode, sharedKafkaTestResource.getKafkaConnectString());
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        tsConsumer = new TieredStorageConsumer<>(props);
        tsConsumer.subscribe(Collections.singleton(TEST_TOPIC_A));

        TopicPartition tta0 = new TopicPartition(TEST_TOPIC_A, 0);
        TopicPartition tta1 = new TopicPartition(TEST_TOPIC_A, 1);
        TopicPartition tta2 = new TopicPartition(TEST_TOPIC_A, 2);

        sendTestData(TEST_TOPIC_A, 0, 100);
        sendTestData(TEST_TOPIC_A, 1, 100);
        sendTestData(TEST_TOPIC_A, 2, 100);

        int[] consumedByPartition = new int[3];
        int totalConsumed = 0;
        int totalRecords = 300;

        while (totalConsumed < totalRecords / 3) {
            ConsumerRecords<String, String> records = tsConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                consumedByPartition[record.partition()]++;
                totalConsumed++;
            }
            if (records.count() > 0) {
                tsConsumer.commitSync();
                assertEquals(consumedByPartition[0], tsConsumer.committed(tta0).offset());
                assertEquals(consumedByPartition[1], tsConsumer.committed(tta1).offset());
                assertEquals(consumedByPartition[2], tsConsumer.committed(tta2).offset());
            }
        }

        long tta0Committed = tsConsumer.committed(tta0).offset();
        long tta1Committed = tsConsumer.committed(tta1).offset();
        long tta2Committed = tsConsumer.committed(tta2).offset();
        long[] committedByPartition = new long[]{tta0Committed, tta1Committed, tta2Committed};

        assertEquals(consumedByPartition[0], tta0Committed);
        assertEquals(consumedByPartition[1], tta1Committed);
        assertEquals(consumedByPartition[2], tta2Committed);

        tsConsumer.close();     // simulate close and restart from committed offsets

        tsConsumer = new TieredStorageConsumer<>(props);
        tsConsumer.subscribe(Collections.singleton(TEST_TOPIC_A));

        boolean[] partitionRecordReceived = new boolean[3];
        while (totalConsumed < totalRecords) {
            ConsumerRecords<String, String> records = tsConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record: records) {
                if (!partitionRecordReceived[record.partition()]) {
                    partitionRecordReceived[record.partition()] = true;
                    assertEquals(committedByPartition[record.partition()], record.offset());
                }
                consumedByPartition[record.partition()]++;
                totalConsumed++;
            }
            tsConsumer.commitSync();
            assertEquals(consumedByPartition[0], tsConsumer.committed(tta0).offset());
            assertEquals(consumedByPartition[1], tsConsumer.committed(tta1).offset());
            assertEquals(consumedByPartition[2], tsConsumer.committed(tta2).offset());
        }

        assertNoMoreRecords(tsConsumer, Duration.ofSeconds(5));
        Map<TopicPartition, OffsetAndMetadata> committed = tsConsumer.committed(new HashSet<>(Arrays.asList(tta0, tta1, tta2)));
        for (TopicPartition tp : committed.keySet()) {
            assertEquals(consumedByPartition[tp.partition()], committed.get(tp).offset());
        }
    }

    private static void assertNoMoreRecords(Consumer<String, String> consumer, Duration checkTime) {
        long begin = System.currentTimeMillis();
        while (System.currentTimeMillis() - begin < checkTime.toMillis()) {
            assertEquals(0, consumer.poll(Duration.ofMillis(100)).count());
        }
    }

    private static TieredStorageConsumer<String, String> getTieredStorageConsumer(TieredStorageConsumer.TieredStorageMode mode) throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        String bootstrapServers = sharedKafkaTestResource.getKafkaConnectString();

        Properties properties = getStandardTieredStorageConsumerProperties(mode, bootstrapServers);
        return new TieredStorageConsumer<>(properties);
    }

    private static Properties getStandardTieredStorageConsumerProperties(TieredStorageConsumer.TieredStorageMode mode, String bootstrapServers) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "tiered-storage-client-id");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "20");
        properties.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "209715200");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.toString());
        properties.setProperty(TieredStorageConsumerConfig.TIERED_STORAGE_MODE_CONFIG, mode.toString());
        properties.setProperty(TieredStorageConsumerConfig.KAFKA_CLUSTER_ID_CONFIG, TEST_CLUSTER);
        properties.setProperty(TieredStorageConsumerConfig.OFFSET_RESET_CONFIG, TieredStorageConsumer.OffsetReset.EARLIEST.toString());
        properties.setProperty(TieredStorageConsumerConfig.STORAGE_SERVICE_ENDPOINT_PROVIDER_CLASS_CONFIG, MockS3StorageServiceEndpointProvider.class.getName());
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return properties;
    }
}
