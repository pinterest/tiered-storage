package com.pinterest.kafka.tieredstorage.common;

public class SegmentUtils {

    /**
     * Get the suffix for a segment file type. For example, the suffix for LOG is ".log".
     * @param fileType
     * @return the suffix for the segment file type
     */
    public static String getFileTypeSuffix(SegmentFileType fileType) {
        return "." + fileType.toString().toLowerCase();
    }

    /**
     * Check if a filename is a segment file.
     * @param filename
     * @return true if the filename is a segment file, false otherwise
     */
    public static boolean isSegmentFile(String filename) {
        return filename.endsWith(getFileTypeSuffix(SegmentFileType.LOG)) ||
                filename.endsWith(getFileTypeSuffix(SegmentFileType.TIMEINDEX)) ||
                filename.endsWith(getFileTypeSuffix(SegmentFileType.INDEX));
    }

    public enum SegmentFileType {
        LOG, INDEX, TIMEINDEX
    }
}
