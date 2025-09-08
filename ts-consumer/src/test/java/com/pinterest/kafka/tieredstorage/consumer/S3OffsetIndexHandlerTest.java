package com.pinterest.kafka.tieredstorage.consumer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
class S3OffsetIndexHandlerTest {
    private static final String INDEX_FILE_NAME = "00000000000000000000.index";
    private ByteBuffer indexFileBuffer;

    @BeforeEach
    void setUp() throws URISyntaxException, IOException {
        Path path = Paths.get(
                Objects.requireNonNull(
                        S3OffsetIndexHandlerTest.class.getClassLoader().getResource(INDEX_FILE_NAME)
                ).toURI()
        );
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            indexFileBuffer = ByteBuffer.allocate((int) channel.size());
            while (channel.read(indexFileBuffer) > 0);
            indexFileBuffer.flip();
        }

    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void getMinimumBytePositionInFile() {
    }

    /**
     * Test getMinimumPositionForOffset method
     */
    @Test
    void getMinimumPositionForOffset() {
        long baseOffset = Long.parseLong(INDEX_FILE_NAME.substring(0, INDEX_FILE_NAME.lastIndexOf(".")));
        assertEquals(0, S3OffsetIndexHandler.getMinimumPositionForOffset(indexFileBuffer, baseOffset, baseOffset));
        assertEquals(0, S3OffsetIndexHandler.getMinimumPositionForOffset(indexFileBuffer, baseOffset, baseOffset + 10));
        assertEquals(0, S3OffsetIndexHandler.getMinimumPositionForOffset(indexFileBuffer, baseOffset, baseOffset + 20));
        assertEquals(0, S3OffsetIndexHandler.getMinimumPositionForOffset(indexFileBuffer, baseOffset, baseOffset + 40));
        assertEquals(4182, S3OffsetIndexHandler.getMinimumPositionForOffset(indexFileBuffer, baseOffset, baseOffset + 41));
        assertEquals(4182, S3OffsetIndexHandler.getMinimumPositionForOffset(indexFileBuffer, baseOffset, baseOffset + 42));
        assertEquals(4182, S3OffsetIndexHandler.getMinimumPositionForOffset(indexFileBuffer, baseOffset, baseOffset + 63));
        assertEquals(4182, S3OffsetIndexHandler.getMinimumPositionForOffset(indexFileBuffer, baseOffset, baseOffset + 81));
        assertEquals(8364, S3OffsetIndexHandler.getMinimumPositionForOffset(indexFileBuffer, baseOffset, baseOffset + 82));
        assertEquals(8364, S3OffsetIndexHandler.getMinimumPositionForOffset(indexFileBuffer, baseOffset, baseOffset + 83));
        assertEquals(8364, S3OffsetIndexHandler.getMinimumPositionForOffset(indexFileBuffer, baseOffset, baseOffset + 90));
        assertEquals(8364, S3OffsetIndexHandler.getMinimumPositionForOffset(indexFileBuffer, baseOffset, baseOffset + 120));
        assertEquals(8364, S3OffsetIndexHandler.getMinimumPositionForOffset(indexFileBuffer, baseOffset, baseOffset + 122));
        assertEquals(12546, S3OffsetIndexHandler.getMinimumPositionForOffset(indexFileBuffer, baseOffset, baseOffset + 124));
        assertEquals(12546, S3OffsetIndexHandler.getMinimumPositionForOffset(indexFileBuffer, baseOffset, baseOffset + 134));
        assertEquals(16728, S3OffsetIndexHandler.getMinimumPositionForOffset(indexFileBuffer, baseOffset, baseOffset + 167));
        assertEquals(16728, S3OffsetIndexHandler.getMinimumPositionForOffset(indexFileBuffer, baseOffset, baseOffset + 199));
        assertEquals(20910, S3OffsetIndexHandler.getMinimumPositionForOffset(indexFileBuffer, baseOffset, baseOffset + 242));
        assertEquals(25092, S3OffsetIndexHandler.getMinimumPositionForOffset(indexFileBuffer, baseOffset, baseOffset + 280));
        assertEquals(25092, S3OffsetIndexHandler.getMinimumPositionForOffset(indexFileBuffer, baseOffset, baseOffset + 286));
        assertEquals(29274, S3OffsetIndexHandler.getMinimumPositionForOffset(indexFileBuffer, baseOffset, baseOffset + 287));
        assertEquals(29274, S3OffsetIndexHandler.getMinimumPositionForOffset(indexFileBuffer, baseOffset, baseOffset + 294));
        // not verifying last covered offset
        assertEquals(29274, S3OffsetIndexHandler.getMinimumPositionForOffset(indexFileBuffer, baseOffset, baseOffset + 295));
        assertEquals(29274, S3OffsetIndexHandler.getMinimumPositionForOffset(indexFileBuffer, baseOffset, baseOffset + 303));
        assertEquals(29274, S3OffsetIndexHandler.getMinimumPositionForOffset(indexFileBuffer, baseOffset, baseOffset + 1126305));
        assertEquals(29274, S3OffsetIndexHandler.getMinimumPositionForOffset(indexFileBuffer, baseOffset, baseOffset + 11126305));
    }

    @Test
    @Disabled
    void testGetOffsetForTime() throws IOException {
        File file = new File(S3OffsetIndexHandlerTest.class.getClassLoader().getResource("log-files/timeindex_test/00000000000000022964.timeindex").getFile());
        try (FileInputStream fis = new FileInputStream(file);
             FileChannel channel = fis.getChannel()) {

            final int ENTRY_SIZE = 12; // 8 bytes for timestamp, 4 for offset
            ByteBuffer buffer = ByteBuffer.allocate(ENTRY_SIZE);

            int entryNum = 0;
            while (channel.read(buffer) == ENTRY_SIZE) {
                buffer.flip();

                long timestamp = buffer.getLong();
                int offset = buffer.getInt();

                System.out.printf("Entry %d: timestamp=%d (%s), relativeOffset=%d\n",
                        entryNum++, timestamp, Instant.ofEpochMilli(timestamp), offset);

                buffer.clear();
            }
        }
    }
}