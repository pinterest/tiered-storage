package com.pinterest.kafka.tieredstorage.common;

public class SegmentUtils {

    public static String getFileTypeSuffix(SegmentFileType fileType) {
        return "." + fileType.toString().toLowerCase();
    }

    public static boolean isSegmentFile(String filename) {
        return filename.endsWith(getFileTypeSuffix(SegmentFileType.LOG)) ||
                filename.endsWith(getFileTypeSuffix(SegmentFileType.TIMEINDEX)) ||
                filename.endsWith(getFileTypeSuffix(SegmentFileType.INDEX));
    }

    public enum SegmentFileType {
        LOG, INDEX, TIMEINDEX
    }
}
