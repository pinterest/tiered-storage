package com.pinterest.kafka.tieredstorage.uploader;

import com.google.common.annotations.VisibleForTesting;
import com.pinterest.kafka.tieredstorage.common.discovery.StorageServiceEndpointProvider;
import com.pinterest.kafka.tieredstorage.common.discovery.s3.S3StorageServiceEndpoint;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.nio.file.NoSuchFileException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * MultiThreadedS3FileUploader uploads files to S3 in parallel using a thread pool.
 * The threadpool size is configurable via tieredstorage.uploader.upload.thread.count
 */
public class MultiThreadedS3FileUploader implements S3FileUploader {
    private static final Logger LOG = LogManager.getLogger(MultiThreadedS3FileUploader.class);
    private static final int UPLOAD_TIMEOUT_ERROR_CODE = 601;
    private static final int UPLOAD_FILE_NOT_FOUND_ERROR_CODE = 602;
    private static final int UPLOAD_GENERAL_ERROR_CODE = 603;
    private final ExecutorService executorService;
    private final StorageServiceEndpointProvider endpointProvider;
    private final Heartbeat heartbeat;
    private static S3Client s3Client;
    private final SegmentUploaderConfiguration config;

    public MultiThreadedS3FileUploader(StorageServiceEndpointProvider endpointProvider, SegmentUploaderConfiguration config, KafkaEnvironmentProvider environmentProvider) {
        this.endpointProvider = endpointProvider;
        this.config = config;
        if (s3Client == null) {
            s3Client = S3Client.builder().build();
        }
        executorService = Executors.newFixedThreadPool(config.getUploadThreadCount());
        heartbeat = new Heartbeat("uploader", config, environmentProvider);
        LOG.info("Started MultiThreadedS3FileUploader with threadpool size=" + config.getUploadThreadCount());
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
        CompletableFuture<PutObjectResponse> future =
                CompletableFuture.supplyAsync(() -> {
                    PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                            .bucket(s3Bucket)
                            .key(s3Key)
                            // Changing checksum algorithm does not seem to
                            // have any impact regarding seeing CPU intensive
                            // sun/security/provider/MD5.implCompress
                            // that is observed in the flame graph.
                            //.checksumAlgorithm(ChecksumAlgorithm.CRC32_C)
                            .build();
                    return s3Client.putObject(putObjectRequest, uploadTask.getAbsolutePath());
                }, executorService).orTimeout(config.getUploadTimeoutMs(), TimeUnit.MILLISECONDS);

        LOG.info(String.format("Submitted upload of s3://%s/%s", s3Bucket, s3Key));
        future.whenComplete((putObjectResponse, throwable) -> {
            long timeSpentMs = System.currentTimeMillis() - queueTime;
            if (throwable != null) {
                LOG.error(String.format("Failed upload of s3://%s/%s in %d ms.", s3Bucket, s3Key, timeSpentMs), throwable);

                int errorCode = getErrorCode(throwable, putObjectResponse);

                s3UploadCallback.onCompletion(
                        uploadTask,
                        timeSpentMs,
                        throwable,
                        errorCode
                );
            } else {
                LOG.info(String.format("Completed upload of s3://%s/%s in %d ms.", s3Bucket, s3Key, timeSpentMs));
                s3UploadCallback.onCompletion(uploadTask, timeSpentMs,null, putObjectResponse.sdkHttpResponse().statusCode());
            }
        });
    }

    private int getErrorCode(Throwable throwable, PutObjectResponse putObjectResponse) {
        if (throwable == null) {
            return putObjectResponse == null ? UPLOAD_GENERAL_ERROR_CODE : putObjectResponse.sdkHttpResponse().statusCode();
        }
        if (throwable instanceof TimeoutException) {
            return UPLOAD_TIMEOUT_ERROR_CODE;
        }
        if (throwable instanceof NoSuchFileException) {
            return UPLOAD_FILE_NOT_FOUND_ERROR_CODE;
        }
        return getErrorCode(throwable.getCause(), putObjectResponse);

    }

    public void stop() {
        s3Client.close();
        executorService.shutdown();
        heartbeat.stop();
    }

    @Override
    public StorageServiceEndpointProvider getStorageServiceEndpointProvider() {
        return endpointProvider;
    }

    @VisibleForTesting
    protected static void overrideS3Client(S3Client newS3Client) {
        s3Client = newS3Client;
    }
}
