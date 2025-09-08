package com.pinterest.kafka.tieredstorage.common.metadata;

import java.time.Instant;

/**
 * Utility class for working with TimeIndex files
 */
public class TimeIndexUtil {
    
    /**
     * Print the contents of a TimeIndex to stdout
     * @param timeIndex the TimeIndex to print
     */
    public static void printTimeIndex(TimeIndex timeIndex) {
        System.out.println("TimeIndex Summary:");
        System.out.println("  Entries: " + timeIndex.size());
        System.out.println("  Empty: " + timeIndex.isEmpty());
        
        if (!timeIndex.isEmpty()) {
            TimeIndex.TimeIndexEntry first = timeIndex.getFirstEntry();
            TimeIndex.TimeIndexEntry last = timeIndex.getLastEntry();
            
            System.out.println("  First entry: " + formatEntry(first));
            System.out.println("  Last entry: " + formatEntry(last));
            
            long timeSpan = last.getTimestamp() - first.getTimestamp();
            System.out.println("  Time span: " + timeSpan + " ms (" + (timeSpan / 1000.0) + " seconds)");
            
            // Print all entries if there are few enough
            if (timeIndex.size() <= 10) {
                System.out.println("\nAll entries:");
                for (int i = 0; i < timeIndex.size(); i++) {
                    TimeIndex.TimeIndexEntry entry = timeIndex.getEntry(i);
                    System.out.printf("  [%d] %s\n", i, formatEntry(entry));
                }
            } else {
                System.out.println("\nFirst 5 entries:");
                for (int i = 0; i < 5; i++) {
                    TimeIndex.TimeIndexEntry entry = timeIndex.getEntry(i);
                    System.out.printf("  [%d] %s\n", i, formatEntry(entry));
                }
                System.out.println("  ...");
                System.out.println("Last 5 entries:");
                for (int i = timeIndex.size() - 5; i < timeIndex.size(); i++) {
                    TimeIndex.TimeIndexEntry entry = timeIndex.getEntry(i);
                    System.out.printf("  [%d] %s\n", i, formatEntry(entry));
                }
            }
        }
    }
    
    /**
     * Format a TimeIndexEntry for human-readable display
     * @param entry the entry to format
     * @return formatted string representation
     */
    public static String formatEntry(TimeIndex.TimeIndexEntry entry) {
        return String.format("timestamp=%d (%s), relativeOffset=%d",
                entry.getTimestamp(),
                Instant.ofEpochMilli(entry.getTimestamp()),
                entry.getRelativeOffset());
    }
    
    /**
     * Find the best entry for a given timestamp and return a human-readable description
     * @param timeIndex the time index to search
     * @param targetTimestamp the timestamp to search for
     * @return description of the result
     */
    public static String findAndDescribeEntry(TimeIndex timeIndex, long targetTimestamp) {
        if (timeIndex.isEmpty()) {
            return "TimeIndex is empty";
        }
        
        TimeIndex.TimeIndexEntry entry = timeIndex.findEntryForTimestamp(targetTimestamp);
        if (entry == null) {
            return String.format("No entry found for timestamp %d (%s) - target is before first entry",
                    targetTimestamp, Instant.ofEpochMilli(targetTimestamp));
        }
        
        return String.format("For timestamp %d (%s), found entry: %s",
                targetTimestamp,
                Instant.ofEpochMilli(targetTimestamp),
                formatEntry(entry));
    }
    
    /**
     * Validate that a TimeIndex is properly formatted (entries sorted by timestamp)
     * @param timeIndex the time index to validate
     * @return true if valid, false otherwise
     */
    public static boolean isValid(TimeIndex timeIndex) {
        if (timeIndex.isEmpty()) {
            return true;
        }
        
        for (int i = 1; i < timeIndex.size(); i++) {
            long prevTimestamp = timeIndex.getEntry(i - 1).getTimestamp();
            long currentTimestamp = timeIndex.getEntry(i).getTimestamp();
            
            if (currentTimestamp < prevTimestamp) {
                System.err.printf("Invalid TimeIndex: entry %d has timestamp %d which is less than previous timestamp %d\n",
                        i, currentTimestamp, prevTimestamp);
                return false;
            }
        }
        
        return true;
    }
}
