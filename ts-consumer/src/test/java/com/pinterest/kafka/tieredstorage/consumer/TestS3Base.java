package com.pinterest.kafka.tieredstorage.consumer;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.pinterest.kafka.tieredstorage.common.Utils;
import org.apache.kafka.common.record.S3Records;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.MockedStatic;
import org.mockito.exceptions.base.MockitoException;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.nio.spi.s3.S3ClientStore;
import software.amazon.nio.spi.s3.S3FileSystemProvider;
import software.amazon.nio.spi.s3.TestS3SeekableByteChannel;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.concurrent.ExecutionException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class TestS3Base extends TestBase {
    protected static final Logger LOG = LogManager.getLogger(TestS3Utils.class.getName());
    protected static final String S3_BUCKET = "test-bucket";
    protected static final String S3_BASE_PREFIX = "retention-3days/tiered_storage_test";
    protected static final String KAFKA_CLUSTER_ID = "test-cluster";
    protected static final String KAFKA_TOPIC = "test_topic";
    protected static final String CONSUMER_GROUP = "test-group";
    protected static final String CLIENT_ID = "tiered-storage-consumer";
    protected static final int MAX_POLL_RECORDS = 20000;
    protected static final long MAX_PARTITION_FETCH_BYTES = 209715200;
    protected static final int TEST_TOPIC_A_P0_NUM_RECORDS = 10995;   // mock test data living in src/test/resources/log-files/test_topic_a-0
    protected static final int TEST_TOPIC_A_P1_NUM_RECORDS = 10019;   // mock test data living in src/test/resources/log-files/test_topic_a-1
    protected static final int TEST_TOPIC_A_P2_NUM_RECORDS = 10249;    // mock test data living in src/test/resources/log-files/test_topic_a-2

    @RegisterExtension
    protected static final S3MockExtension S3_MOCK =
            S3MockExtension.builder().silent().withSecureConnection(false).build();
    protected S3Client s3Client;
    protected S3AsyncClient s3AsyncClient;
    protected enum FileType {
        LOG, INDEX, TIMEINDEX
    }
    private static MockedStatic<S3Records> s3RecordsMockedStatic;

    @BeforeEach
    void setup() throws InterruptedException, IOException, KeeperException, ExecutionException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        s3Client = S3Client.builder()
                .endpointOverride(URI.create(S3_MOCK.getServiceEndpoint()))
                .region(Region.US_EAST_1)
                .credentialsProvider(AnonymousCredentialsProvider.create())
                .build();
        s3Client.createBucket(CreateBucketRequest.builder().bucket(S3_BUCKET).build());
        s3AsyncClient = S3AsyncClient.builder()
                .endpointOverride(URI.create(S3_MOCK.getServiceEndpoint()))
                .region(Region.US_EAST_1)
                .credentialsProvider(AnonymousCredentialsProvider.create())
                .build();
    }

    @AfterEach
    @Override
    void tearDown() throws IOException, ExecutionException, InterruptedException {
        super.tearDown();
        clearAllObjects();
        s3Client.close();
    }

    protected void prepareS3Mocks() throws IOException {
        S3FileSystemProvider s3FileSystemProvider = mock(S3FileSystemProvider.class);
        when(s3FileSystemProvider.getFileSystem(any())).thenCallRealMethod();
        when(s3FileSystemProvider.newByteChannel(any(), any())).thenAnswer(invocation -> new TestS3SeekableByteChannel(invocation.getArgument(0), s3AsyncClient));
        S3ClientStore s3ClientStore = mock(S3ClientStore.class);
        when(s3ClientStore.getAsyncClientForBucketName(any())).thenReturn(s3AsyncClient);
        try {
            s3RecordsMockedStatic = mockStatic(S3Records.class);
            s3RecordsMockedStatic.when(S3Records::getS3FileSystemProvider).thenReturn(s3FileSystemProvider);
            s3RecordsMockedStatic.when(() -> S3Records.open(anyString(), anyString(), anyInt(), anyBoolean(), anyBoolean(), anyInt(), anyBoolean())).thenCallRealMethod();
        } catch (MockitoException e) {
            // ignore the exception if the mock is already open
            LOG.info("Failed to open S3Records mock; Please disregard this message if all tests pass", e);
        }
    }

    protected void closeS3Mocks() {
        if (s3RecordsMockedStatic != null) {
            s3RecordsMockedStatic.close();
        }
    }

    protected void clearAllObjects() {
        s3Client.listObjectsV2Paginator(builder -> builder.bucket(S3_BUCKET).prefix(S3_BASE_PREFIX)).stream().forEach(page -> {
            page.contents().forEach(object -> {
                s3Client.deleteObject(builder -> builder.bucket(S3_BUCKET).key(object.key()));
            });
        });
    }

    protected void putEmptyObjects(String cluster, String topic, int partition, long minOffset, long maxOffset, long numOffsetsPerFile) {
        for (long i = minOffset; i <= maxOffset; i += numOffsetsPerFile) {
            LOG.info(String.format("Put empty object to bucket=%s, key=%s", S3_BUCKET, getS3ObjectKey(cluster, topic, partition, i, TestS3Utils.FileType.INDEX)));
            s3Client.putObject(PutObjectRequest.builder().bucket(S3_BUCKET).key(getS3ObjectKey(cluster, topic, partition, i, TestS3Utils.FileType.INDEX)).build(), RequestBody.empty());
            LOG.info(String.format("Put empty object to bucket=%s, key=%s", S3_BUCKET, getS3ObjectKey(cluster, topic, partition, i, TestS3Utils.FileType.LOG)));
            s3Client.putObject(PutObjectRequest.builder().bucket(S3_BUCKET).key(getS3ObjectKey(cluster, topic, partition, i, TestS3Utils.FileType.LOG)).build(), RequestBody.empty());
            LOG.info(String.format("Put empty object to bucket=%s, key=%s", S3_BUCKET, getS3ObjectKey(cluster, topic, partition, i, TestS3Utils.FileType.TIMEINDEX)));
            s3Client.putObject(PutObjectRequest.builder().bucket(S3_BUCKET).key(getS3ObjectKey(cluster, topic, partition, i, TestS3Utils.FileType.TIMEINDEX)).build(), RequestBody.empty());
        }
    }

    protected void putEmptyObjects(String cluster, String topic, int partition, long minOffset, long maxOffset, long numOffsetsPerFile, int prefixEntropyNumDigits) {
        for (long i = minOffset; i <= maxOffset; i += numOffsetsPerFile) {
            s3Client.putObject(PutObjectRequest.builder().bucket(S3_BUCKET).key(getS3ObjectKey(cluster, topic, partition, i, TestS3Utils.FileType.INDEX, prefixEntropyNumDigits)).build(), RequestBody.empty());
            s3Client.putObject(PutObjectRequest.builder().bucket(S3_BUCKET).key(getS3ObjectKey(cluster, topic, partition, i, TestS3Utils.FileType.LOG, prefixEntropyNumDigits)).build(), RequestBody.empty());
            s3Client.putObject(PutObjectRequest.builder().bucket(S3_BUCKET).key(getS3ObjectKey(cluster, topic, partition, i, TestS3Utils.FileType.TIMEINDEX, prefixEntropyNumDigits)).build(), RequestBody.empty());
        }
    }

    protected void putEmptyObjects(String topic, int partition, long minOffset, long maxOffset, long numOffsetsPerFile) {
        putEmptyObjects(KAFKA_CLUSTER_ID, topic, partition, minOffset, maxOffset, numOffsetsPerFile);
    }

    protected void putObjects(String cluster, String topic, int partition, String directory) {
        File directoryFile = new File(directory);
        File[] filesToUpload = directoryFile.listFiles();
        if (filesToUpload == null) {
            return;
        }
        for (File file : filesToUpload) {
            String key = getS3ObjectKey(cluster, topic, partition, file.getName());
            s3Client.putObject(PutObjectRequest.builder().bucket(S3_BUCKET).key(key).build(), RequestBody.fromFile(file));
            LOG.info(String.format("Put %s to bucket=%s, key=%s", file.getAbsolutePath(), S3_BUCKET, key));
        }
    }

    protected static String getS3ObjectKey(String topic, int partition, long offset, TestS3Utils.FileType fileType) {
        return getS3ObjectKey(KAFKA_CLUSTER_ID, topic, partition, offset, fileType);
    }

    protected static String getS3ObjectKey(String cluster, String topic, int partition, long offset, TestS3Utils.FileType fileType) {
        return S3_BASE_PREFIX + "/" + cluster + "/" + topic + "-" + partition + "/" + S3Utils.getZeroPaddedOffset(offset) + "." + fileType.toString().toLowerCase();
    }

    protected static String getS3ObjectKey(String cluster, String topic, int partition, long offset, TestS3Utils.FileType fileType, int prefixEntropyNumDigits) {
        return S3_BASE_PREFIX + "/" + Utils.getBinaryHashForClusterTopicPartition(cluster, topic, partition, prefixEntropyNumDigits) +
                "/" + cluster + "/" + topic + "-" + partition + "/" + S3Utils.getZeroPaddedOffset(offset) + "." + fileType.toString().toLowerCase();
    }

    protected static String getS3ObjectKey(String cluster, String topic, int partition, String filename) {
        return S3_BASE_PREFIX + "/" + cluster + "/" + topic + "-" + partition + "/" + filename;
    }

    protected static String getS3BasePrefixWithCluster() {
        return "s3://" + S3_BUCKET + "/" + S3_BASE_PREFIX + "/" + KAFKA_CLUSTER_ID;
    }
}
