package com.pinterest.kafka.tieredstorage.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class ExampleLocalKafkaProducer {
    private static final Logger LOG = LogManager.getLogger(ExampleLocalKafkaProducer.class.getName());
    private static final String TOPIC_OPT = "t";
    private static final String NUM_MESSAGES_OPT = "n";
    
    public static void main(String[] args) throws ParseException {

        Options options = new Options();

        Option topicOpt = new Option(TOPIC_OPT, "the topic to produce to");
        topicOpt.setRequired(true);
        topicOpt.setArgs(1);
        options.addOption(topicOpt);

        Option numMessagesOpt = new Option(NUM_MESSAGES_OPT, "number of messages to produce");
        numMessagesOpt.setRequired(true);
        numMessagesOpt.setArgs(1);
        options.addOption(numMessagesOpt);

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        String topic = cmd.getOptionValue(TOPIC_OPT);
        long numMessages = Long.parseLong(cmd.getOptionValue(NUM_MESSAGES_OPT));

        LOG.info(String.format("Starting ExampleLocalKafkaProducer with topic=%s numMessages=%s", topic, numMessages));

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        long numMessagesSent = 0L;
        while (numMessagesSent < numMessages) {
            String message = generateMessage();
            producer.send(new ProducerRecord<String,String>(topic, message));
            numMessagesSent++;
            if (numMessagesSent % 1000L == 0) {
                LOG.info(String.format("Sent %s messages so far", numMessagesSent));
            }
        }

        LOG.info(String.format("Sent %s messages to %s", numMessagesSent, topic));
        producer.close();
    }

    private static String generateMessage() {
        return "Hello world! Generated at " + System.currentTimeMillis();
    }
}
