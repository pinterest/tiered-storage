package com.pinterest.kafka.tieredstorage.consumer;

/**
 * A class to hold all the consumer metrics
 */
public class ConsumerMetrics {

    private static final String CONSUMER_METRIC_PREFIX = "tieredstorage.consumer";
    public static final String OFFSET_CONSUMED_LATEST_METRIC = CONSUMER_METRIC_PREFIX + "." + "offset.consumed.latest";
    public static final String OFFSET_CONSUMED_TOTAL_METRIC = CONSUMER_METRIC_PREFIX + "." + "offset.consumed.total";
    public static final String OFFSET_CONSUMPTION_MISSED_METRIC = CONSUMER_METRIC_PREFIX + "." + "offset.consumption.missed";
    public static final String S3_LIST_OBJECTS_LATENCY_METRIC = CONSUMER_METRIC_PREFIX + "." + "s3.listObjects.latency.ms";
    public static final String S3_LIST_OBJECTS_CALLS_METRIC = CONSUMER_METRIC_PREFIX + "." + "s3.listObjects.calls";
    public static final String CONSUMER_POLL_TIME_MS_METRIC = CONSUMER_METRIC_PREFIX + "." + "poll.time.ms";
}
