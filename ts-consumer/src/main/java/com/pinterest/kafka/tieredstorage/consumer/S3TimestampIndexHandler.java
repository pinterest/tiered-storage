package com.pinterest.kafka.tieredstorage.consumer;

import com.pinterest.kafka.tieredstorage.common.SegmentUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.nio.ByteBuffer;

public class S3TimestampIndexHandler {
    private static final Logger LOG = LogManager.getLogger(S3TimestampIndexHandler.class.getName());
    private static final int INDEX_ENTRY_SIZE = 12;
    private static final int TIMESTAMP_SIZE = 8;
    private static final int OFFSET_POINTER_SIZE = 4;
    private static final S3Client s3Client = S3Client.builder().region(S3Utils.REGION).build();

    public static Long getFirstTimestampInFile(Triple<String, String, Long> s3Path) {
        ByteBuffer timeIndexFileByteBuffer = getFileContentInByteBuffer(s3Path);
        return timeIndexFileByteBuffer.getLong();
    }

    public static Long getLastTimestampInFile(Triple<String, String, Long> s3Path) {
        ByteBuffer timeIndexFileByteBuffer = getFileContentInByteBuffer(s3Path);
        return timeIndexFileByteBuffer.getLong(s3Path.getRight().intValue() - INDEX_ENTRY_SIZE);
    }

    public static int getOffsetPointerForEntry(Triple<String, String, Long> s3Path, int entryNumber) {
        ByteBuffer timeIndexFileByteBuffer = getFileContentInByteBuffer(s3Path);
        return timeIndexFileByteBuffer.getInt(entryNumber * INDEX_ENTRY_SIZE + TIMESTAMP_SIZE);
    }

    public static int getLastOffsetPointer(Triple<String, String, Long> s3Path) {
        ByteBuffer timeIndexFileByteBuffer = getFileContentInByteBuffer(s3Path);
        return timeIndexFileByteBuffer.getInt(s3Path.getRight().intValue() - OFFSET_POINTER_SIZE);
    }

    private static ByteBuffer getFileContentInByteBuffer(Triple<String, String, Long> s3Path) {
        String logFileKey = s3Path.getMiddle();
        if (!logFileKey.endsWith(SegmentUtils.getFileTypeSuffix(SegmentUtils.SegmentFileType.TIMEINDEX))) {
            throw new RuntimeException(String.format("logFileKey %s must end with .timeindex", logFileKey));
        }
        String timeIndexFileKey = logFileKey; //.substring(0, logFileKey.length() - 4);
        GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(s3Path.getLeft()).key(timeIndexFileKey).build();
        //LOG.info("Object request: {}", getObjectRequest);
        byte[] timeIndexFileBytes = s3Client.getObject(getObjectRequest, ResponseTransformer.toBytes()).asByteArray();
        return ByteBuffer.wrap(timeIndexFileBytes);
    }

    /**
     * Records in timeindex file:
     *          8 bytes: timestamp
     *          4 bytes: offset pointer to corresponding index file
     */
    protected static int getIndexFilePositionForTimestamp(Triple<String, String, Long> s3Path, long timestamp) {
        ByteBuffer timeIndexFileByteBuffer = getFileContentInByteBuffer(s3Path);
        int numIndexRecords = timeIndexFileByteBuffer.remaining() / INDEX_ENTRY_SIZE;
        int rangeStart = 0;
        int rangeEnd = numIndexRecords - 1;
        int midRange = (rangeStart + rangeEnd) / 2;

        while (true) {
            int timestampAtMidRange = timeIndexFileByteBuffer.getInt(INDEX_ENTRY_SIZE * midRange);
            if (timestampAtMidRange == timestamp) {
                int positionInIndexFile = timeIndexFileByteBuffer.getInt(INDEX_ENTRY_SIZE * midRange + TIMESTAMP_SIZE);
                LOG.info("timestamp: {}, positionInIndexFile: {}", timestamp, positionInIndexFile);
                return positionInIndexFile;
            }

            if (timestamp < timestampAtMidRange)
                rangeEnd = midRange - 1;
            else
                rangeStart = midRange;

            if (rangeStart == rangeEnd) {
                int positionInIndexFile = timeIndexFileByteBuffer.getInt(INDEX_ENTRY_SIZE * rangeStart + TIMESTAMP_SIZE);
                LOG.info("timestamp: {}, positionInIndexFile: {}", timestamp, positionInIndexFile);
                return positionInIndexFile;
            }

            midRange = (rangeStart + rangeEnd + 1) / 2;
        }
    }
}
