package com.pinterest.kafka.tieredstorage.uploader;

import com.google.common.annotations.VisibleForTesting;
import com.pinterest.kafka.tieredstorage.common.discovery.StorageServiceEndpointProvider;
import com.pinterest.kafka.tieredstorage.common.discovery.s3.S3StorageServiceEndpoint;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * S3FileDownloader downloads files from S3, mainly to check for watermark files to get the committed offsets
 */
public class S3FileDownloader {
    private static final Logger LOG = LogManager.getLogger(S3FileDownloader.class);
    private final StorageServiceEndpointProvider endpointProvider;
    private static S3Client s3Client;
    private final SegmentUploaderConfiguration config;

    public S3FileDownloader(StorageServiceEndpointProvider endpointProvider, SegmentUploaderConfiguration config) {
        if (s3Client == null) {
            s3Client = S3Client.builder().build();
        }
        this.endpointProvider = endpointProvider;
        this.config = config;
    }

    public String getOffsetFromWatermarkFile(TopicPartition topicPartition) {
        S3StorageServiceEndpoint.Builder endpointBuilder =
                (S3StorageServiceEndpoint.Builder) endpointProvider.getStorageServiceEndpointBuilderForTopic(topicPartition.topic());
        S3StorageServiceEndpoint endpoint = endpointBuilder
                .setTopicPartition(topicPartition)
                .setPrefixEntropyNumBits(config.getS3PrefixEntropyBits())
                .build();

        String bucket = endpoint.getBucket();
        String key = endpoint.getFullPrefix() + "/" + WatermarkFileHandler.WATERMARK_FILE_NAME;
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();
        try (ResponseInputStream<GetObjectResponse> response = s3Client.getObject(request)) {
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(response, StandardCharsets.UTF_8));
            String line = reader.readLine();
            reader.close();
            return line == null ? null : line.trim();
        } catch (IOException | NoSuchKeyException e) {
            LOG.warn(String.format("No watermark file (at %s) was found for %s.", request.key(), topicPartition));
        }
        return null;
    }

    @VisibleForTesting
    protected static void overrideS3Client(S3Client newS3Client) {
        s3Client = newS3Client;
    }
}
