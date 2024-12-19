package com.pinterest.kafka.tieredstorage.uploader;

import com.google.common.annotations.VisibleForTesting;
import com.pinterest.kafka.tieredstorage.common.discovery.StorageServiceEndpointProvider;
import com.pinterest.kafka.tieredstorage.common.discovery.s3.S3StorageServiceEndpoint;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.nio.file.NoSuchFileException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

/**
 * MultiThreadedS3FileUploader uploads files to S3 in parallel using a thread pool.
 * The threadpool size is configurable via tieredstorage.uploader.upload.thread.count
 */
public class MultiThreadedS3FileUploader implements S3FileUploader {
    private static final Logger LOG = LogManager.getLogger(MultiThreadedS3FileUploader.class);
    protected static final int UPLOAD_TIMEOUT_ERROR_CODE = 601;
    protected static final int UPLOAD_FILE_NOT_FOUND_ERROR_CODE = 602;
    protected static final int UPLOAD_GENERAL_ERROR_CODE = 603;
    private final ExecutorService executorService;
    private final StorageServiceEndpointProvider endpointProvider;
    private final Heartbeat heartbeat;
    private static S3AsyncClient s3AsyncClient;
    private final SegmentUploaderConfiguration config;

    public MultiThreadedS3FileUploader(StorageServiceEndpointProvider endpointProvider, SegmentUploaderConfiguration config, KafkaEnvironmentProvider environmentProvider) {
        this.endpointProvider = endpointProvider;
        this.config = config;
        ClientOverrideConfiguration overrideConfiguration = ClientOverrideConfiguration.builder()
                .apiCallTimeout(Duration.ofMillis(config.getUploadTimeoutMs()))
                .build();
        if (s3AsyncClient == null) {
            s3AsyncClient = S3AsyncClient.builder().overrideConfiguration(overrideConfiguration).build();
        }
        executorService = Executors.newFixedThreadPool(config.getUploadThreadCount());
        heartbeat = new Heartbeat("uploader", config, environmentProvider);
        LOG.info("Started MultiThreadedS3FileUploader with threadpool size=" + config.getUploadThreadCount() + " and timeout=" + config.getUploadTimeoutMs() + "ms");
    }

    @Override
    public void uploadFile(DirectoryTreeWatcher.UploadTask uploadTask, S3UploadCallback s3UploadCallback) {
        S3StorageServiceEndpoint.Builder endpointBuilder =
                (S3StorageServiceEndpoint.Builder) endpointProvider.getStorageServiceEndpointBuilderForTopic(uploadTask.getTopicPartition().topic());
        S3StorageServiceEndpoint endpoint = endpointBuilder
                .setTopicPartition(uploadTask.getTopicPartition())
                .setPrefixEntropyNumBits(config.getS3PrefixEntropyBits())
                .build();
        String s3Bucket = endpoint.getBucket();
        String s3Prefix = endpoint.getPrefixExcludingTopicPartition();
        String subpath = uploadTask.getFullFilename().endsWith(".wm") ?
                uploadTask.getSubPath().replace("/" + uploadTask.getOffset(), "/offset") :
                uploadTask.getSubPath();

        String s3Key = String.format("%s/%s", s3Prefix, subpath);
        long queueTime = System.currentTimeMillis();
        String uploadPathString = String.format("s3://%s/%s", s3Bucket, s3Key);
        LOG.info(String.format("Submitting upload of %s --> %s", uploadTask.getAbsolutePath(), uploadPathString));
        executorService.submit(() -> {
            CompletableFuture<PutObjectResponse> future;
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(s3Bucket)
                    .key(s3Key)
                    // Changing checksum algorithm does not seem to
                    // have any impact regarding seeing CPU intensive
                    // sun/security/provider/MD5.implCompress
                    // that is observed in the flame graph.
                    //.checksumAlgorithm(ChecksumAlgorithm.CRC32_C)
                    .build();
            uploadTask.setUploadDestinationPathString(uploadPathString);    // set the upload destination path so that it can be used in the callback
            try {
                future = s3AsyncClient.putObject(putObjectRequest, uploadTask.getAbsolutePath());
            } catch (Exception e) {
                long timeSpentMs = System.currentTimeMillis() - queueTime;
                LOG.warn(String.format("Caught exception during putObject for %s --> %s in %dms", uploadTask.getAbsolutePath(), uploadPathString, timeSpentMs), e);
                int errorCode = UPLOAD_GENERAL_ERROR_CODE;
                if (Utils.isAssignableFromRecursive(e, NoSuchFileException.class)) {
                    errorCode = UPLOAD_FILE_NOT_FOUND_ERROR_CODE;
                }
                s3UploadCallback.onCompletion(uploadTask, timeSpentMs, e, errorCode);
                return;
            }
            future.whenComplete((putObjectResponse, throwable) -> {
                long timeSpentMs = System.currentTimeMillis() - queueTime;
                if (throwable != null) {
                    LOG.warn(String.format("PutObject failed for %s --> %s in %d ms.", uploadTask.getAbsolutePath(), uploadPathString, timeSpentMs), throwable);

                    int errorCode = getErrorCode(throwable, putObjectResponse);

                    s3UploadCallback.onCompletion(
                            uploadTask,
                            timeSpentMs,
                            throwable,
                            errorCode
                    );
                } else {
                    LOG.info(String.format("Completed upload of %s in %d ms.", uploadPathString, timeSpentMs));
                    s3UploadCallback.onCompletion(uploadTask, timeSpentMs,null, putObjectResponse.sdkHttpResponse().statusCode());
                }
            });
        });
    }

    private int getErrorCode(Throwable throwable, PutObjectResponse putObjectResponse) {
        if (throwable == null) {
            return putObjectResponse == null ? UPLOAD_GENERAL_ERROR_CODE : putObjectResponse.sdkHttpResponse().statusCode();
        }
        if (throwable instanceof ApiCallTimeoutException || throwable instanceof TimeoutException) {
            return UPLOAD_TIMEOUT_ERROR_CODE;
        }
        if (throwable instanceof NoSuchFileException) {
            return UPLOAD_FILE_NOT_FOUND_ERROR_CODE;
        }
        return getErrorCode(throwable.getCause(), putObjectResponse);

    }

    public void stop() {
        s3AsyncClient.close();
        executorService.shutdown();
        heartbeat.stop();
    }

    @Override
    public StorageServiceEndpointProvider getStorageServiceEndpointProvider() {
        return endpointProvider;
    }

    @VisibleForTesting
    protected static void overrideS3Client(S3AsyncClient newS3Client) {
        s3AsyncClient = newS3Client;
    }
}
