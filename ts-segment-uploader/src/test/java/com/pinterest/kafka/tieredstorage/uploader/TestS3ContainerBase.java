package com.pinterest.kafka.tieredstorage.uploader;

import com.pinterest.kafka.tieredstorage.common.SegmentUtils;
import com.pinterest.kafka.tieredstorage.common.Utils;
import com.pinterest.kafka.tieredstorage.common.discovery.s3.S3StorageServiceEndpoint;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ExecutionException;

@Testcontainers
public class TestS3ContainerBase {
    private static final Logger LOG = LogManager.getLogger(TestBase.class.getName());
    protected static final String S3_BUCKET = "test-bucket";
    protected static final String TEST_CLUSTER = "test-cluster-base";
    protected static final String TEST_TOPIC_A = "test_topic_a";
    @Container
    public static final GenericContainer<?> S3_MOCK_CONTAINER = new GenericContainer<>(DockerImageName.parse("adobe/s3mock:4.2.0"))
            .withExposedPorts(9090) // http
            .withEnv("initialBuckets", S3_BUCKET)
            .withEnv("debug", "false");

    protected S3Client s3Client;
    protected S3AsyncClient s3AsyncClient;
    private static String endpoint;

    @BeforeEach
    public void setup() throws Exception {
        endpoint = String.format("http://%s:%d",
                S3_MOCK_CONTAINER.getHost(),
                S3_MOCK_CONTAINER.getMappedPort(9090));
        s3Client = S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.US_EAST_1)
                .credentialsProvider(AnonymousCredentialsProvider.create())
                .forcePathStyle(true)
                .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED)
                .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
                .build();
        s3AsyncClient = S3AsyncClient.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.US_EAST_1)
                .credentialsProvider(AnonymousCredentialsProvider.create())
                .forcePathStyle(true)
                .build();
    }

    @AfterEach
    public void tearDown() throws IOException, InterruptedException, ExecutionException {
        Thread.sleep(5000);
        clearAllObjects(S3_BUCKET);
        s3Client.close();
    }

    protected static ResponseInputStream<GetObjectResponse> getObjectResponse(String bucket, String key, S3Client s3Client) {
        return s3Client.getObject(GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build()
        );
    }

    protected static ListObjectsV2Response getListObjectsV2Response(String bucket, String prefix, S3AsyncClient s3AsyncClient) throws ExecutionException, InterruptedException {
        return s3AsyncClient.listObjectsV2(ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).build()).get();
    }

    protected static PutObjectResponse putObjectResponse(String bucket, String prefix, S3Client s3Client, String content) {
        return s3Client.putObject(PutObjectRequest.builder().bucket(bucket).key(prefix).build(), RequestBody.fromBytes(content.getBytes(StandardCharsets.UTF_8)));
    }

    protected void putEmptyObjects(long minOffset, long maxOffset, long numOffsetsPerFile, S3StorageServiceEndpoint endpoint) {
        for (long i = minOffset; i <= maxOffset; i += numOffsetsPerFile) {
            LOG.info(String.format("Put empty object to bucket=%s, key=%s", endpoint.getBucket(), getS3ObjectKey(endpoint, i, SegmentUtils.SegmentFileType.INDEX)));
            s3Client.putObject(PutObjectRequest.builder().bucket(endpoint.getBucket()).key(getS3ObjectKey(endpoint, i, SegmentUtils.SegmentFileType.INDEX)).build(), RequestBody.empty());

            LOG.info(String.format("Put empty object to bucket=%s, key=%s", endpoint.getBucket(), getS3ObjectKey(endpoint, i, SegmentUtils.SegmentFileType.LOG)));
            s3Client.putObject(PutObjectRequest.builder().bucket(endpoint.getBucket()).key(getS3ObjectKey(endpoint, i, SegmentUtils.SegmentFileType.LOG)).build(), RequestBody.empty());

            LOG.info(String.format("Put empty object to bucket=%s, key=%s", endpoint.getBucket(), getS3ObjectKey(endpoint, i, SegmentUtils.SegmentFileType.TIMEINDEX)));
            s3Client.putObject(PutObjectRequest.builder().bucket(endpoint.getBucket()).key(getS3ObjectKey(endpoint, i, SegmentUtils.SegmentFileType.TIMEINDEX)).build(), RequestBody.empty());
        }
    }

    public static S3AsyncClient getS3AsyncClientWithCustomApiCallTimeout(long timeoutMs) {
        ClientOverrideConfiguration overrideConfiguration = ClientOverrideConfiguration.builder()
                .apiCallTimeout(Duration.ofMillis(1L))
                .build();
        return S3AsyncClient.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.US_EAST_1)
                .credentialsProvider(AnonymousCredentialsProvider.create())
                .overrideConfiguration(overrideConfiguration)
                .build();
    }

    protected static String getS3ObjectKey(S3StorageServiceEndpoint endpoint, long offset, SegmentUtils.SegmentFileType fileType) {
        return endpoint.getFullPrefix() + "/" + Utils.getZeroPaddedOffset(offset) + "." + fileType.toString().toLowerCase();
    }

    public static SegmentUploaderConfiguration getSegmentUploaderConfiguration(String clusterName) throws IOException {
        return new SegmentUploaderConfiguration("src/test/resources", clusterName);
    }

    public void clearAllObjects(String bucket) {
        s3Client.listObjectsV2Paginator(builder -> builder.bucket(bucket).prefix("/")).stream().forEach(page -> {
            page.contents().forEach(object -> {
                s3Client.deleteObject(builder -> builder.bucket(bucket).key(object.key()));
            });
        });
    }
}
