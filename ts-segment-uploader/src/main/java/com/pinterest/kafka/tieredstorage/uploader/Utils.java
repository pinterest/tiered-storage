package com.pinterest.kafka.tieredstorage.uploader;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Utils class for common utilities for the uploader
 */
public class Utils {
    private final static Logger LOG = LogManager.getLogger(Utils.class);
    public final static String LOCK_FILE_NAME = "ts.lock";
    public final static Path LOCK_FILE_PATH = WatermarkFileHandler.BASE_TS_DIRECTORY.resolve(LOCK_FILE_NAME);
    private static RandomAccessFile file;
    private static FileChannel channel;
    private static FileLock lock;

    public static void createOrClearFolder(File folder) {
        if (folder.isFile())
            return;

        if (!folder.exists()) {
            folder.mkdirs();
            return;
        }

        File[] files = folder.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isDirectory()) {
                    createOrClearFolder(f);
                }
                f.delete();
            }
        }
    }

    private static void createLockFileIfMissing() throws IOException {
        if (LOCK_FILE_PATH.toFile().exists())
            return;

        File tempFile = Files.createFile(LOCK_FILE_PATH).toFile();
        FileWriter fileWriter = new FileWriter(tempFile, false);
        fileWriter.write("0");
        fileWriter.close();
    }

    /**
     * Acquire a lock for the current uploader process.
     * At most one uploader process should be running per-broker at any given time.
     */
    public static void acquireLock() {
        try {
            createLockFileIfMissing();
            file = new RandomAccessFile(LOCK_FILE_PATH.toString(), "rw");
            channel = file.getChannel();
            lock = channel.tryLock();

            if (lock == null) {
                LOG.error("Another process is holding the lock.");
                channel.close();
                file.close();
                throw new RuntimeException("Another process is holding the lock. Exiting");
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to acquire the lock.", e);
        }
    }

    public static void releaseLock() throws IOException {
        lock.release();
        channel.close();
        file.close();
    }
}

