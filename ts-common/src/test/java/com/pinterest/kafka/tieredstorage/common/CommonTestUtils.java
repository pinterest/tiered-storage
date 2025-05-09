package com.pinterest.kafka.tieredstorage.common;

import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CommonTestUtils {

    private static final Logger LOG  = LogManager.getLogger(CommonTestUtils.class.getName());
    public static final String HEADER1_KEY = "header1";
    public static final String HEADER2_KEY = "header2";
    public static final String KAFKA_HEADER_1_VAL = "kafkaHeader1-val";
    public static final String KAFKA_HEADER_2_VAL = "kafkaHeader2-val";
    public static final String KAFKA_VAL_PREFIX = "kafkaVal-";
    public static final String HEADER1_VAL = "header1-val";
    public static final String HEADER2_VAL = "header2-val";
    public static final String VAL_PREFIX = "val-";

    public enum RecordContentType {
        TIERED_STORAGE, KAFKA
    }

    public static void writeExpectedRecordFormatTestData(RecordContentType type, SharedKafkaTestResource sharedKafkaTestResource, String topic, int partition, int numRecords) {
        long numRecordsSent = 0;
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, sharedKafkaTestResource.getKafkaConnectString());
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "0");
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        KafkaProducer<byte[], byte[]> kafkaProducer = new KafkaProducer<>(props);
        LOG.info(String.format("Going to produce %d records to topic %s, partition %d.", numRecords, topic, partition));
        while (numRecordsSent < numRecords) {
            RecordHeaders recordHeaders = new RecordHeaders();
            String header1Val = type == RecordContentType.KAFKA ? KAFKA_HEADER_1_VAL : HEADER1_VAL;
            String header2Val = type == RecordContentType.KAFKA ? KAFKA_HEADER_2_VAL : HEADER2_VAL;
            String valPrefix = type == RecordContentType.KAFKA ? KAFKA_VAL_PREFIX : VAL_PREFIX;
            recordHeaders.add(new RecordHeader(HEADER1_KEY, header1Val.getBytes()));
            recordHeaders.add(new RecordHeader(HEADER2_KEY, header2Val.getBytes()));
            /*
             * Record looks like:
             * ----- RecordContentType: KAFKA --------------
             * Headers: {header1=kafkaHeader1-val, header2=kafkaHeader2-val}
             * Key: [OFFSET]
             * Value: kafkaVal-[OFFSET]
             * ---------------------------------------------
             *
             * ----- RecordContentType: TIERED_STORAGE -----
             * Headers: {header1=header1-val, header2=header2-val}
             * Key: [OFFSET]
             * Value: val-[OFFSET]
             * ---------------------------------------------
             */
            ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(topic, partition, String.valueOf(numRecordsSent).getBytes(), (valPrefix + numRecordsSent).getBytes(), recordHeaders);
            kafkaProducer.send(producerRecord);
            kafkaProducer.flush();
            numRecordsSent++;
        }
        LOG.info(String.format("Produced %d records to topic %s, partition %d.", numRecords, topic, partition));
        kafkaProducer.flush();
        kafkaProducer.close();
    }

    public static void validateRecordContent(RecordContentType expectedType, ConsumerRecord<String, String> record) {
        long offset = record.offset();
        String expectedKey = String.valueOf(offset);
        String expectedVal = expectedType == RecordContentType.KAFKA ? KAFKA_VAL_PREFIX + offset : VAL_PREFIX + offset;
        String expectedHeader1Val = expectedType == RecordContentType.KAFKA ? KAFKA_HEADER_1_VAL : HEADER1_VAL;
        String expectedHeader2Val = expectedType == RecordContentType.KAFKA ? KAFKA_HEADER_2_VAL : HEADER2_VAL;
        assertEquals(expectedKey, record.key());
        assertEquals(expectedVal, record.value());
        assertEquals(HEADER1_KEY, record.headers().headers(HEADER1_KEY).iterator().next().key());
        assertEquals(HEADER2_KEY, record.headers().headers(HEADER2_KEY).iterator().next().key());
        assertEquals(expectedHeader1Val, new String(record.headers().headers(HEADER1_KEY).iterator().next().value()));
        assertEquals(expectedHeader2Val, new String(record.headers().headers(HEADER2_KEY).iterator().next().value()));
    }

}
