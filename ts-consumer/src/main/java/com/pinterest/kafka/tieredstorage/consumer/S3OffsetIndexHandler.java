package com.pinterest.kafka.tieredstorage.consumer;

import com.google.common.annotations.VisibleForTesting;
import com.pinterest.kafka.tieredstorage.common.SegmentUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.nio.ByteBuffer;

/**
 * S3OffsetIndexHandler is a utility class that provides helper methods for reading S3 offset index files.
 */
public class S3OffsetIndexHandler {
    private static final Logger LOG = LogManager.getLogger(S3OffsetIndexHandler.class.getName());
    private static final int INDEX_ENTRY_SIZE = 8;
    private Triple<String, String, Long> currentS3Path = null;
    private Long currentBaseOffset = null;
    private ByteBuffer indexFileByteBuffer = null;
    private static S3Client s3Client = S3Client.builder().region(S3Utils.REGION).build();

    /**
     * Returns the Kafka offset at the given byte position in the log segment.
     * @param s3Path S3 path of the log segment
     * @param position byte position in the log segment
     * @return Kafka offset at the given byte position in the log segment
     */
    public static long getOffsetAtPosition(Triple<String, String, Long> s3Path, int position) {
        String logFileKey = s3Path.getMiddle();
        String noExtensionFileKey = logFileKey.substring(0, logFileKey.lastIndexOf("."));
        long baseOffset = Long.parseLong(noExtensionFileKey.substring(noExtensionFileKey.lastIndexOf("/") + 1));
        String indexFileKey = noExtensionFileKey + SegmentUtils.getFileTypeSuffix(SegmentUtils.SegmentFileType.INDEX);
        GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(s3Path.getLeft()).key(indexFileKey).build();
        byte[] indexFileBytes = s3Client.getObject(getObjectRequest, ResponseTransformer.toBytes()).asByteArray();
        ByteBuffer indexFileByteBuffer = ByteBuffer.wrap(indexFileBytes);
        return baseOffset + indexFileByteBuffer.getInt(position);
    }

    /**
     * Returns the minimum byte position in the log segment given the Kafka offset of interest.
     * @param s3Path S3 path of the log segment
     * @param offsetOfInterest Kafka offset of interest
     * @return minimum byte position in the log segment based on the given offset of interest
     */
    public int getMinimumBytePositionInFile(Triple<String, String, Long> s3Path, long offsetOfInterest) {
        if (currentS3Path != s3Path || indexFileByteBuffer == null || currentBaseOffset == null) {
            String logFileKey = s3Path.getMiddle();
            if (!logFileKey.endsWith(SegmentUtils.getFileTypeSuffix(SegmentUtils.SegmentFileType.LOG)))
                throw new RuntimeException(String.format("logFileKey %s should end with .log", logFileKey));
            String noExtensionFileKey = logFileKey.substring(0, logFileKey.length() - 4);
            currentBaseOffset = Long.parseLong(noExtensionFileKey.substring(noExtensionFileKey.lastIndexOf("/") + 1));
            String indexFileKey = noExtensionFileKey + SegmentUtils.getFileTypeSuffix(SegmentUtils.SegmentFileType.INDEX);
            GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(s3Path.getLeft()).key(indexFileKey).build();
            byte[] indexFileBytes = s3Client.getObject(getObjectRequest, ResponseTransformer.toBytes()).asByteArray();
            indexFileByteBuffer = ByteBuffer.wrap(indexFileBytes);
            currentS3Path = s3Path;
        }
        LOG.debug(String.format("Processing index file for %s", s3Path));
        LOG.debug(String.format("Getting minimum position for offset: currentBaseOffset=%s, offsetOfInterest=%s", currentBaseOffset, offsetOfInterest));
        return getMinimumPositionForOffset(indexFileByteBuffer, currentBaseOffset, offsetOfInterest);
    }

    /**
     * Records in index file:
     *          4 bytes: relative offset
     *          4 bytes: position in log segment
     * @return minimum byte position in the log segment of 'baseOffset' to look for 'offsetOfInterest'
     */
    protected static int getMinimumPositionForOffset(ByteBuffer indexFileByteBuffer, long baseOffset, long offsetOfInterest) {
        int relativeOffset = (int) (offsetOfInterest - baseOffset);
        int numIndexRecords = indexFileByteBuffer.remaining() / INDEX_ENTRY_SIZE;
        int rangeStart = 0;
        int rangeEnd = Math.max(0, numIndexRecords - 1);
        int midRange = (rangeStart + rangeEnd) / 2;

        if (indexFileByteBuffer.capacity() == 0) {
            // empty index file
            return 0;
        }

        while (true) {
            int relativeOffsetAtMidRange = indexFileByteBuffer.getInt(INDEX_ENTRY_SIZE * midRange);
            if (relativeOffsetAtMidRange == relativeOffset) {
                int positionInLogSegment = indexFileByteBuffer.getInt(INDEX_ENTRY_SIZE * midRange + 4);
                LOG.debug(String.format("baseOffset: %s, offsetOfInterest: %s, closestIndexOffset: %s, positionInLogSegment: %s",
                        baseOffset, offsetOfInterest, baseOffset + relativeOffsetAtMidRange, positionInLogSegment));
                return positionInLogSegment;
            }

            if (rangeStart == rangeEnd) {
                if (rangeStart == 0 && relativeOffset < relativeOffsetAtMidRange) {
                    LOG.debug(String.format("baseOffset: %s, offsetOfInterest: %s, closestIndexOffset: %s, positionInLogSegment: %s",
                            baseOffset, offsetOfInterest, baseOffset + rangeStart, 0));
                    return 0;
                }
                int positionInLogSegment = indexFileByteBuffer.getInt(INDEX_ENTRY_SIZE * rangeStart + 4);
                LOG.debug(String.format("baseOffset: %s, offsetOfInterest: %s, closestIndexOffset: %s, positionInLogSegment: %s",
                        baseOffset, offsetOfInterest, baseOffset + rangeStart, positionInLogSegment));
                return positionInLogSegment;
            }

            if (relativeOffset < relativeOffsetAtMidRange)
                rangeEnd = midRange - 1;
            else
                rangeStart = midRange;

            midRange = (rangeStart + rangeEnd + 1) / 2;
        }
    }

    @VisibleForTesting
    protected static void overrideS3Client(S3Client newS3Client) {
        s3Client = newS3Client;
    }
}
