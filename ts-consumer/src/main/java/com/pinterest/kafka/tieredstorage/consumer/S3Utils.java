package com.pinterest.kafka.tieredstorage.consumer;

import com.google.common.annotations.VisibleForTesting;
import com.pinterest.kafka.tieredstorage.common.SegmentUtils;
import com.pinterest.kafka.tieredstorage.common.Utils;
import com.pinterest.kafka.tieredstorage.common.metrics.MetricRegistryManager;
import com.pinterest.kafka.tieredstorage.common.metrics.MetricsConfiguration;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * S3Utils is a utility class that provides helper methods for S3 operations.
 */
public class S3Utils {
    private static final Logger LOG = LogManager.getLogger(S3Utils.class.getName());
    public static final Region REGION = Region.US_EAST_1;
    private static final String S3_PATH_REGEX = "s3:\\/\\/([^\\/]+)\\/(.+)";
    private static final Pattern pattern = Pattern.compile(S3_PATH_REGEX, Pattern.MULTILINE);
    private static S3Client s3Client = S3Client.builder().region(REGION).build();

    /**
     * Returns the file name from the S3 key
     * @param key
     * @return file name
     */
    public static String getFileNameFromKey(String key) {
        return key.substring(key.lastIndexOf("/") + 1);
    }

    /**
     * Returns a {@link Triple} representing the bucket, prefix and offset path for the given S3 location and topic partition
     * @param location
     * @param topicPartition
     * @param offset
     * @return {@link Triple} representing the bucket, prefix and offset path
     */
    public static Triple<String, String, String> getPartitionOffsetPath(String location, TopicPartition topicPartition, Long offset) {
        String topic = topicPartition.topic();
        int partition = topicPartition.partition();

        String bucket, prefix;
        Matcher matcher = pattern.matcher(location);
        if (!matcher.find() || matcher.groupCount() != 2) {
            throw S3Exception.builder().message("Invalid S3 path " + location +
                    ". Expected format: s3://bucket/prefix").build();
        }
        bucket = matcher.group(1);
        prefix = matcher.group(2);
        if (!prefix.endsWith("/")) {
            // add trailing backslash
            prefix = prefix + "/";
        }

        return Triple.of(
                bucket,
                String.format("%s%s-%d/", prefix, topic, partition),
                offset == null ? "" : Utils.getZeroPaddedOffset(offset)
        );
    }

    /**
     * Returns a {@link Pair} representing the bucket and prefix for the given S3 location and topic partition
     * @param location
     * @param topicPartition
     * @return {@link Pair} representing the bucket and prefix
     */
    public static Pair<String, String> getPartitionPath(String location, TopicPartition topicPartition) {
        Triple<String, String, String> triple = getPartitionOffsetPath(location, topicPartition, null);
        return Pair.of(
                triple.getLeft(),
                triple.getMiddle()
        );
    }

    /**
     * Returns a {@link ListObjectsV2Iterable} representing the list of S3 Objects for the given request
     * @param topicPartition
     * @param request
     * @param metricsConfiguration
     * @return {@link ListObjectsV2Iterable} representing the list of S3 Objects
     */
    private static ListObjectsV2Iterable listObjectsPaginated(TopicPartition topicPartition, ListObjectsV2Request request, MetricsConfiguration metricsConfiguration) {
        long ts = System.currentTimeMillis();
        ListObjectsV2Iterable result = s3Client.listObjectsV2Paginator(request);
        MetricRegistryManager.getInstance(metricsConfiguration).updateHistogram(
                topicPartition.topic(),
                topicPartition.partition(),
                ConsumerMetrics.S3_LIST_OBJECTS_LATENCY_METRIC,
                System.currentTimeMillis() - ts,
                "ts=true", "bucket=" + request.bucket(), "prefix=" + request.prefix()
        );
        MetricRegistryManager.getInstance(metricsConfiguration).incrementCounter(
                topicPartition.topic(),
                topicPartition.partition(),
                ConsumerMetrics.S3_LIST_OBJECTS_CALLS_METRIC,
                "ts=true", "bucket=" + request.bucket(), "prefix=" + request.prefix()
        );
        return result;
    }

    /**
     * Returns a {@link ListObjectsV2Request.Builder} representing the request to list S3 objects for the given bucket, prefix for objects
     * after the given objectToStartAfter in alphanumeric order
     * @param bucket
     * @param prefix
     * @param objectToStartAfter
     * @return {@link ListObjectsV2Request.Builder} representing the request to list S3 objects
     */
    private static ListObjectsV2Request.Builder getListObjectsRequestBuilder(String bucket, String prefix, String objectToStartAfter) {
        return objectToStartAfter != null ?
                ListObjectsV2Request
                        .builder()
                        .bucket(bucket)
                        .prefix(prefix)
                        .startAfter(objectToStartAfter) :
                ListObjectsV2Request
                        .builder()
                        .bucket(bucket)
                        .prefix(prefix);
    }

    /**
     * Returns a sorted map of offset keys to S3 object paths for the given location, topic partition, offset key and latest S3 object
     * @param location
     * @param topicPartition
     * @param offsetKey
     * @param latestS3Object
     * @param metricsConfiguration
     * @return sorted map of offset keys to S3 object paths
     */
    public static TreeMap<Long, Triple<String, String, Long>> getSortedOffsetKeyMap(String location, TopicPartition topicPartition, String offsetKey, String latestS3Object, MetricsConfiguration metricsConfiguration) {
        Pair<String, String> s3Path = getPartitionPath(location, topicPartition);
        LOG.debug("s3Path to use for creating offset key map: " + s3Path);
        // https://docs.aws.amazon.com/AmazonS3/latest/userguide/ListingKeysUsingAPIs.html
        // List results are always returned in UTF-8 binary order.
        TreeMap<Long, Triple<String, String, Long>> sortedOffsetKeyMap = new TreeMap<>();

        ListObjectsV2Request.Builder requestBuilder = getListObjectsRequestBuilder(s3Path.getLeft(), s3Path.getRight(), latestS3Object);
        ListObjectsV2Iterable result = listObjectsPaginated(topicPartition, requestBuilder.build(), metricsConfiguration);
        String lastSkippedKey = null;
        long lastSizeBytes = -1;
        Map<Long, Integer> offsetToCountMap = new HashMap<>();
        for (S3Object s3Object : result.contents()) {
            String key = s3Object.key();
            if (!SegmentUtils.isSegmentFile(key)) {
                LOG.debug(String.format("Skipping S3 object %s for topicPartition=%s in %s", key, topicPartition, s3Path));
                continue;
            }
            String filename = S3Utils.getFileNameFromKey(key);
            Long offset = Long.parseLong(filename.substring(0, filename.indexOf(".")));
            offsetToCountMap.put(offset, offsetToCountMap.getOrDefault(offset, 0) + 1);
            if (!key.endsWith(SegmentUtils.getFileTypeSuffix(SegmentUtils.SegmentFileType.LOG)))
                continue;
            if (filename.compareTo(offsetKey) < 0) {
                lastSkippedKey = key;
                lastSizeBytes = s3Object.size();
                continue;
            }

            sortedOffsetKeyMap.put(
                    Long.parseLong(
                            key.substring(s3Path.getRight().length(), key.indexOf(".", s3Path.getRight().length()))
                    ),
                    Triple.of(s3Path.getLeft(), key, s3Object.size())
            );
        }

        // add the last-skipped file as it may contain offset of interest
        if (lastSkippedKey != null) {
            sortedOffsetKeyMap.put(
                    Long.parseLong(
                            lastSkippedKey.substring(s3Path.getRight().length(), lastSkippedKey.indexOf(".", s3Path.getRight().length()))
                    ),
                    Triple.of(s3Path.getLeft(), lastSkippedKey, lastSizeBytes)
            );
        }
        TreeSet<Long> validOffsets = offsetToCountMap.entrySet().stream().filter(entry -> entry.getValue() == 3 && sortedOffsetKeyMap.containsKey(entry.getKey()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toCollection(TreeSet::new));
        if (validOffsets.size() != sortedOffsetKeyMap.size()) {
            LOG.warn(String.format("Found %s valid offsets but %s S3 objects for topicPartition=%s in %s. This may indicate dangling objects in S3. Dangling offsets: %s", validOffsets.size(), sortedOffsetKeyMap.size(),
                    topicPartition, s3Path, sortedOffsetKeyMap.keySet().stream().filter(key -> !validOffsets.contains(key)).collect(Collectors.toList())));
            // remove dangling objects
            sortedOffsetKeyMap.keySet().retainAll(validOffsets);
        }
        LOG.info(String.format("Retrieved %s S3 objects [%s, %s].", sortedOffsetKeyMap.size(), sortedOffsetKeyMap.firstEntry(), sortedOffsetKeyMap.lastEntry()));
        return sortedOffsetKeyMap;
    }
    @VisibleForTesting
    protected static void overrideS3Client(S3Client newS3Client) {
        s3Client = newS3Client;
    }
}
