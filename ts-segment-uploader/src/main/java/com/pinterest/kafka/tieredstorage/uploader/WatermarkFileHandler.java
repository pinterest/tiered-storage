package com.pinterest.kafka.tieredstorage.uploader;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * WatermarkFileHandler is a class that handles watermark files (committed uploaded offsets) for the tiered storage uploader
 */
public class WatermarkFileHandler {
    private static final Logger LOG = LogManager.getLogger(WatermarkFileHandler.class);
    public final static Path BASE_TS_DIRECTORY = new File("/tmp/tieredstorage/").toPath();
    public final static Path WATERMARK_DIRECTORY = BASE_TS_DIRECTORY.resolve("watermarks/");
    public final static String WATERMARK_FILE_NAME = "offset.wm";
    public final static String WATERMARK_FILE_EXTENSION = ".wm";

    static {
        Utils.createOrClearFolder(WATERMARK_DIRECTORY.toFile());
    }

    public Path getWatermarkFile(String topicPartition, String content) {
        Path filePath = null;
        try {
            Path directoryPath = WATERMARK_DIRECTORY.resolve(topicPartition);
            File directory = directoryPath.toFile();
            if (!directory.exists()) {
                if (!directory.mkdirs() && !directory.exists())
                    // check again if directory.exists() in case of contending threads creating it
                    throw new IOException("Could not create directory " + directory);
            }
            filePath = directoryPath.resolve(content + WATERMARK_FILE_EXTENSION);

            Files.deleteIfExists(filePath);
            File tempFile = Files.createFile(filePath).toFile();
            FileWriter fileWriter = new FileWriter(tempFile, false);
            fileWriter.write(content);
            fileWriter.close();
            return tempFile.toPath();
        } catch (IOException e) {
            LOG.error(String.format("Failed to create the watermark file %s for %s (%s).", filePath, topicPartition, content), e);
            return null;
        }
    }

    public void deleteWatermarkFile(Path watermarkFile) {
        if (!watermarkFile.toFile().delete())
            LOG.warn(String.format("Could not delete file %s.", watermarkFile));
    }
}
