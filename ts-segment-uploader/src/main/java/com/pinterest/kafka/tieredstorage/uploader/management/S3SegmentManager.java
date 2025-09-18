package com.pinterest.kafka.tieredstorage.uploader.management;

import com.google.common.annotations.VisibleForTesting;
import com.pinterest.kafka.tieredstorage.common.SegmentUtils;
import com.pinterest.kafka.tieredstorage.common.Utils;
import com.pinterest.kafka.tieredstorage.common.discovery.StorageServiceEndpointProvider;
import com.pinterest.kafka.tieredstorage.common.discovery.s3.S3StorageServiceEndpoint;
import com.pinterest.kafka.tieredstorage.common.metadata.TopicPartitionMetadata;
import com.pinterest.kafka.tieredstorage.common.metrics.MetricRegistryManager;
import com.pinterest.kafka.tieredstorage.uploader.KafkaEnvironmentProvider;
import com.pinterest.kafka.tieredstorage.uploader.SegmentUploaderConfiguration;
import com.pinterest.kafka.tieredstorage.uploader.UploaderMetrics;
import com.pinterest.kafka.tieredstorage.uploader.leadership.LeadershipWatcher;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * {@link SegmentManager} implementation that uses S3 as the underlying storage system.
 */
public class S3SegmentManager extends SegmentManager {
    private final static Logger LOG = LogManager.getLogger(S3SegmentManager.class.getName());
    private static S3Client s3Client;

    /**
     * Create a new S3-backed SegmentManager.
     *
     * <p>Initializes the underlying {@link S3Client} with an API call timeout derived from the
     * uploader configuration. Also triggers superclass initialization which may schedule
     * background garbage collection.</p>
     *
     * @param config uploader configuration
     * @param environmentProvider provider for cluster and broker identity
     * @param endpointProvider provider for storage service endpoints
     * @param leadershipWatcher watcher that reports the set of leading partitions
     */
    public S3SegmentManager(SegmentUploaderConfiguration config, KafkaEnvironmentProvider environmentProvider, StorageServiceEndpointProvider endpointProvider, LeadershipWatcher leadershipWatcher) {
        super(config, environmentProvider, endpointProvider, leadershipWatcher);
        ClientOverrideConfiguration overrideConfiguration = ClientOverrideConfiguration.builder()
                .apiCallTimeout(Duration.ofMillis(config.getUploadTimeoutMs()))
                .build();
        if (s3Client == null) {
            s3Client = S3Client.builder().overrideConfiguration(overrideConfiguration).build();
        }
    }

    /**
     * Perform S3-specific initialization logic. Currently logs an initialization message.
     */
    @Override
    public void initialize() {
        LOG.info("Initializing S3SegmentManager");
    }

    /**
     * Load {@link TopicPartitionMetadata} for the provided topic-partition from S3.
     *
     * <p>Reads the metadata file from the configured S3 prefix. If found, the eTag from the
     * response is stored in the metadata as a load hash to enable conditional updates.</p>
     *
     * @param topicPartition topic-partition whose metadata should be loaded
     * @return the loaded metadata, or null if the metadata file does not exist
     * @throws IOException if an I/O error occurs while reading the object
     */
    @Override
    public synchronized TopicPartitionMetadata getTopicPartitionMetadataFromStorage(TopicPartition topicPartition) throws IOException {
        S3StorageServiceEndpoint endpoint = getS3StorageServiceEndpoint(topicPartition);
        String bucket = endpoint.getBucket();
        String key = endpoint.getFullPrefix();
        GetObjectRequest request = GetObjectRequest.builder().bucket(bucket).key(key + "/" + TopicPartitionMetadata.FILENAME).build();
        try {
            long start = System.currentTimeMillis();
            try (ResponseInputStream<GetObjectResponse> responseStream = s3Client.getObject(request);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(responseStream, StandardCharsets.UTF_8))) {
                String content = reader.lines().collect(Collectors.joining("\n"));
                GetObjectResponse response = responseStream.response();
                String eTag = response.eTag();

                TopicPartitionMetadata tpMetadata = TopicPartitionMetadata.loadFromJson(content);
                tpMetadata.setLoadHash(eTag);
                LOG.info(String.format("Retrieved TopicPartitionMetadata from %s in %sms", endpoint.getFullPrefixUri() + "/" + TopicPartitionMetadata.FILENAME, System.currentTimeMillis() - start));
                return tpMetadata;
            }
        } catch (NoSuchKeyException e) {
            LOG.warn(String.format("Cannot find metadata file under %s", endpoint.getFullPrefixUri()));
            return null;
        }
    }

    /**
     * Write {@link TopicPartitionMetadata} to S3, using the stored eTag for optimistic concurrency when available.
     *
     * @param tpMetadata metadata to write
     * @return true if the metadata was successfully written, false otherwise
     */
    @Override
    public synchronized boolean writeMetadataToStorage(TopicPartitionMetadata tpMetadata) {
        S3StorageServiceEndpoint endpoint = getS3StorageServiceEndpoint(tpMetadata.getTopicPartition());
        String bucket = endpoint.getBucket();
        String key = endpoint.getFullPrefix() + "/" + TopicPartitionMetadata.FILENAME;
        PutObjectRequest.Builder builder = PutObjectRequest.builder().bucket(bucket).key(key);
        if (tpMetadata.getLoadHash() != null) {
            builder.ifMatch(tpMetadata.getLoadHash());
        } else {
            LOG.warn(String.format("Skipping eTag match validation in metadata update for topicPartition=%s since it is null", tpMetadata.getTopicPartition()));
        }
        PutObjectRequest request = builder.build();
        try {
            long startTs = System.currentTimeMillis();
            PutObjectResponse response = s3Client.putObject(request, RequestBody.fromBytes(tpMetadata.getAsJsonString().getBytes(StandardCharsets.UTF_8)));
            LOG.info(String.format("Wrote TopicPartitionMetadata to %s in %sms", endpoint.getFullPrefixUri() + "/" + TopicPartitionMetadata.FILENAME, System.currentTimeMillis() - startTs));
            return response.sdkHttpResponse().statusCode() == 200;
        } catch (S3Exception s3e) {
            if (s3e.statusCode() == 412) {
                LOG.error(String.format("Failed to write metadata to endpoint %s because the object has been modified since it was loaded.", endpoint.getFullPrefixUri()), s3e);
            } else {
                LOG.error(String.format("Failed to write metadata to endpoint %s", endpoint.getFullPrefixUri()), s3e);
            }
            return false;
        } catch (Exception e) {
            LOG.error(String.format("Failed to write metadata to endpoint %s", endpoint.getFullPrefixUri()), e);
            return false;
        }
    }

    /**
     * Delete all segment files in S3 with base offsets less than or equal to the provided offset.
     *
     * <p>Segments are discovered via a list operation and deleted in ascending order. Deletion
     * stops early if a full set of expected files (log, index, timeindex) is not removed to
     * avoid gaps.</p>
     *
     * @param topicPartition topic-partition whose segments should be deleted
     * @param baseOffset inclusive upper bound for base offsets to delete
     * @return the set of base offsets actually deleted
     */
    @Override
    public synchronized Set<Long> deleteSegmentsBeforeBaseOffsetInclusive(TopicPartition topicPartition, long baseOffset) {
        // list all objects in topic-partition prefix which is less than or equal to baseOffset
        TreeSet<Long> toDeleteSegments = new TreeSet<>();
        S3StorageServiceEndpoint endpoint = getS3StorageServiceEndpoint(topicPartition);
        String bucket = endpoint.getBucket();
        String key = endpoint.getFullPrefix();
        ListObjectsRequest request = ListObjectsRequest.builder().bucket(bucket).prefix(key + "/").build(); // add trailing slash to ensure we don't accidentally delete objects in other partitions with the same prefix
        ListObjectsResponse response = s3Client.listObjects(request);
        for (S3Object object : response.contents()) {
            SegmentUtils.SegmentFileType fileType = Utils.getSegmentFileTypeFromName(object.key());
            if (fileType != null) {
                Optional<Long> offset = Utils.getBaseOffsetFromFilename(object.key());
                if (offset.isPresent() && offset.get() <= baseOffset) {
                    toDeleteSegments.add(offset.get());
                }
            }
        }

        LOG.info(String.format("To delete segments for topicPartition=%s: %s", topicPartition, toDeleteSegments));

        TreeSet<Long> actualDeleted = new TreeSet<>();
        
        // delete objects "atomically" in ascending order of offset - note that S3 does not guarantee atomicity
        for (long offset : toDeleteSegments) {
            ObjectIdentifier logIdentifier = ObjectIdentifier.builder().key(key + "/" + Utils.getZeroPaddedOffset(offset) + "." + SegmentUtils.SegmentFileType.LOG.name().toLowerCase()).build();
            ObjectIdentifier indexIdentifier = ObjectIdentifier.builder().key(key + "/" + Utils.getZeroPaddedOffset(offset) + "." + SegmentUtils.SegmentFileType.INDEX.name().toLowerCase()).build();
            ObjectIdentifier timeIndexIdentifier = ObjectIdentifier.builder().key(key + "/" + Utils.getZeroPaddedOffset(offset) + "." + SegmentUtils.SegmentFileType.TIMEINDEX.name().toLowerCase()).build();
            DeleteObjectsRequest deleteRequest = DeleteObjectsRequest.builder().bucket(bucket).delete(Delete.builder().objects(logIdentifier, indexIdentifier, timeIndexIdentifier).build()).build();
            DeleteObjectsResponse deleteResponse = s3Client.deleteObjects(deleteRequest);
            int deleted = deleteResponse.deleted().size();
            if (deleted != 3) {
                // short circuit to prevent holes / gaps in the segments
                MetricRegistryManager.getInstance(config.getMetricsConfiguration()).incrementCounter(
                    topicPartition.topic(),
                    topicPartition.partition(),
                    UploaderMetrics.SEGMENT_MANAGER_GC_DELETION_FAILURE_COUNT_METRIC,
                    "cluster=" + environmentProvider.clusterId(),
                    "broker=" + environmentProvider.brokerId()
                );
                LOG.warn(String.format("Short-circuiting GC cycle for %s because we only deleted %s objects for offset %s: %s", topicPartition, deleted, offset, deleteResponse.deleted()));
                break;
            } else {
                actualDeleted.add(offset);
                LOG.info(String.format("Deleted %s objects for offset %s: %s", deleted, offset, deleteResponse.deleted()));
            }
        }
        if (!actualDeleted.isEmpty()) {
            LOG.info(String.format("Completed deletion of [%s, %s] segments for topicPartition=%s", actualDeleted.first(), actualDeleted.last(), topicPartition));
        } else {
            LOG.info(String.format("Did not delete any segments for topicPartition=%s", topicPartition));
        }
        return actualDeleted;

    }

    /**
     * Build the {@link S3StorageServiceEndpoint} for a specific topic-partition using the configured
     * prefix entropy and endpoint builder provided by discovery.
     *
     * @param topicPartition topic-partition for which to build the endpoint
     * @return the constructed S3 storage service endpoint
     */
    private S3StorageServiceEndpoint getS3StorageServiceEndpoint(TopicPartition topicPartition) {
        S3StorageServiceEndpoint.Builder endpointBuilder =
        (S3StorageServiceEndpoint.Builder) endpointProvider.getStorageServiceEndpointBuilderForTopic(topicPartition.topic());
        S3StorageServiceEndpoint endpoint = endpointBuilder
                .setTopicPartition(topicPartition)
                .setPrefixEntropyNumBits(config.getS3PrefixEntropyBits())
                .build();
        return endpoint;
    }

    /**
     * Inject a custom {@link S3Client} for testing.
     *
     * @param suppliedS3Client the client to use for subsequent operations
     */
    @VisibleForTesting
    public static void setS3Client(S3Client suppliedS3Client) {
        s3Client = suppliedS3Client;
    }
}
