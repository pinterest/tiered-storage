package com.pinterest.kafka.tieredstorage.uploader;

import com.pinterest.kafka.tieredstorage.common.discovery.StorageServiceEndpointProvider;

public interface S3FileUploader {
    void uploadFile(DirectoryTreeWatcher.UploadTask uploadTask, S3UploadCallback callback);

    void stop();

    StorageServiceEndpointProvider getStorageServiceEndpointProvider();
}