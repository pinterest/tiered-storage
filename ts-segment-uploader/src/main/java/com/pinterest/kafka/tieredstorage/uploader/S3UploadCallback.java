package com.pinterest.kafka.tieredstorage.uploader;

public interface S3UploadCallback {
    void onCompletion(DirectoryTreeWatcher.UploadTask uploadTask, long totalTimeMs, Throwable throwable, int statusCode);
}
