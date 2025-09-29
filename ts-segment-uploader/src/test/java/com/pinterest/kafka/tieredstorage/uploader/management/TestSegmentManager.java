package com.pinterest.kafka.tieredstorage.uploader.management;

import com.pinterest.kafka.tieredstorage.common.SegmentUtils;
import com.pinterest.kafka.tieredstorage.common.Utils;
import com.pinterest.kafka.tieredstorage.common.discovery.StorageServiceEndpointProvider;
import com.pinterest.kafka.tieredstorage.common.discovery.s3.MockS3StorageServiceEndpointProvider;
import com.pinterest.kafka.tieredstorage.common.metadata.TimeIndex;
import com.pinterest.kafka.tieredstorage.common.metadata.TopicPartitionMetadata;
import com.pinterest.kafka.tieredstorage.common.metadata.TopicPartitionMetadataUtil;
import com.pinterest.kafka.tieredstorage.uploader.KafkaEnvironmentProvider;
import com.pinterest.kafka.tieredstorage.uploader.SegmentUploaderConfiguration;
import com.pinterest.kafka.tieredstorage.uploader.TestBase;
import com.pinterest.kafka.tieredstorage.uploader.leadership.LeadershipWatcher;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static com.pinterest.kafka.tieredstorage.uploader.TestBase.TEST_CLUSTER;
import static com.pinterest.kafka.tieredstorage.uploader.TestBase.TEST_TOPIC_A;
import static com.pinterest.kafka.tieredstorage.uploader.TestBase.createTestEnvironmentProvider;
import static com.pinterest.kafka.tieredstorage.uploader.TestBase.getSegmentUploaderConfiguration;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

public class TestSegmentManager {
    private static final File DIRECTORY = new File("/tmp/tiered-storage");
    private SegmentManager mockManager;
    private MockS3StorageServiceEndpointProvider endpointProvider;
    private KafkaEnvironmentProvider environmentProvider;

    @BeforeEach
    public void setup() throws Exception {

        // environment provider setup
        environmentProvider = createTestEnvironmentProvider("sampleZkConnect", "sampleLogDir");
        environmentProvider.load();

        // endpoint provider setup
        endpointProvider = new MockS3StorageServiceEndpointProvider();
        endpointProvider.initialize(TEST_CLUSTER);

        TestBase.deleteDirectory(DIRECTORY.toPath());
        Files.createDirectories(DIRECTORY.toPath());
    }

    @AfterEach
    public void tearDown() throws IOException {
        TestBase.deleteDirectory(DIRECTORY.toPath());
    }

    /**
     * Test that garbage collection works as expected.
     * @throws IOException
     */
    @Test
    public void testGarbageCollection() throws IOException {
        LeadershipWatcher leadershipWatcher = Mockito.mock(LeadershipWatcher.class);
        TopicPartition tp = new TopicPartition(TEST_TOPIC_A, 0);
        when(leadershipWatcher.getLeadingPartitions()).thenReturn(Collections.singleton(tp));

        mockManager = new MockSegmentManager(getSegmentUploaderConfiguration(TEST_CLUSTER), environmentProvider, endpointProvider, leadershipWatcher);
        Files.createDirectories(Paths.get(DIRECTORY.getPath(), tp.toString()));

        // write mock metadata
        TopicPartitionMetadata metadata = new TopicPartitionMetadata(tp);   // retention is 3600 seconds (1 hour)
        TimeIndex timeIndex = new TimeIndex(TimeIndex.TimeIndexType.TOPIC_PARTITION);
        timeIndex.insertEntry(new TimeIndex.TimeIndexEntry(System.currentTimeMillis() - 7200 * 1000, 100, 0L)); // 2 hours ago
        timeIndex.insertEntry(new TimeIndex.TimeIndexEntry(System.currentTimeMillis() - 7200 * 1000, 100, 100L));   // 2 hours ago
        timeIndex.insertEntry(new TimeIndex.TimeIndexEntry(System.currentTimeMillis() - 5400 * 1000, 100, 200L));   // 1.5 hours ago
        timeIndex.insertEntry(new TimeIndex.TimeIndexEntry(System.currentTimeMillis() - 1800 * 1000, 100, 300L)); // 0.5 hours ago
        timeIndex.insertEntry(new TimeIndex.TimeIndexEntry(System.currentTimeMillis(), 100, 400L)); // now
        metadata.updateMetadata(TopicPartitionMetadata.TIMEINDEX_KEY, timeIndex);
        mockManager.writeMetadataToStorage(metadata);

        // write mock data
        Set<Long> offsets = Set.of(0L, 100L, 200L, 300L, 400L, 500L);
        for (long offset : offsets) {
            // write empty segments
            Files.write(Paths.get(DIRECTORY.getPath(), tp.toString(), String.format("%020d", offset) + SegmentUtils.getFileTypeSuffix(SegmentUtils.SegmentFileType.LOG)), "".getBytes(StandardCharsets.UTF_8));
            Files.write(Paths.get(DIRECTORY.getPath(), tp.toString(), String.format("%020d", offset) + SegmentUtils.getFileTypeSuffix(SegmentUtils.SegmentFileType.TIMEINDEX)), "".getBytes(StandardCharsets.UTF_8));
            Files.write(Paths.get(DIRECTORY.getPath(), tp.toString(), String.format("%020d", offset) + SegmentUtils.getFileTypeSuffix(SegmentUtils.SegmentFileType.INDEX)), "".getBytes(StandardCharsets.UTF_8));
        }
        Files.write(Paths.get(DIRECTORY.getPath(), tp.toString(), "offset.wm"), "".getBytes(StandardCharsets.UTF_8));

        File[] files = Paths.get(DIRECTORY.getPath(), tp.toString()).toFile().listFiles();
        assertEquals(offsets.size() * 3 + 2, files.length);

        // run cycle
        mockManager.runGarbageCollection();

        // check new metadata
        TopicPartitionMetadata newMetadata = mockManager.getTopicPartitionMetadataFromStorage(tp);
        assertEquals(tp, newMetadata.getTopicPartition());
        assertEquals(2, newMetadata.getTimeIndex().size());
        assertFalse(newMetadata.getTimeIndex().getEntriesCopy().stream().anyMatch(e -> e.getBaseOffset() == 0L));
        assertFalse(newMetadata.getTimeIndex().getEntriesCopy().stream().anyMatch(e -> e.getBaseOffset() == 100L));
        assertFalse(newMetadata.getTimeIndex().getEntriesCopy().stream().anyMatch(e -> e.getBaseOffset() == 200L));
        assertTrue(newMetadata.getTimeIndex().getEntriesCopy().stream().anyMatch(e -> e.getBaseOffset() == 300L));
        assertTrue(newMetadata.getTimeIndex().getEntriesCopy().stream().anyMatch(e -> e.getBaseOffset() == 400L));

        // check segments
        Set<File> filesAfterGc = Set.of(Paths.get(DIRECTORY.getPath(), tp.toString()).toFile().listFiles());
        assertEquals(3 * 3 + 2, filesAfterGc.size());
        assertTrue(filesAfterGc.stream().anyMatch(f -> f.getName().equals("offset.wm")));
        assertTrue(filesAfterGc.stream().anyMatch(f -> f.getName().equals("_metadata")));
        assertTrue(filesAfterGc.stream().anyMatch(f -> f.getName().equals(String.format("%020d", 300L) + SegmentUtils.getFileTypeSuffix(SegmentUtils.SegmentFileType.LOG))));
        assertTrue(filesAfterGc.stream().anyMatch(f -> f.getName().equals(String.format("%020d", 300L) + SegmentUtils.getFileTypeSuffix(SegmentUtils.SegmentFileType.INDEX))));
        assertTrue(filesAfterGc.stream().anyMatch(f -> f.getName().equals(String.format("%020d", 300L) + SegmentUtils.getFileTypeSuffix(SegmentUtils.SegmentFileType.TIMEINDEX))));
        assertTrue(filesAfterGc.stream().anyMatch(f -> f.getName().equals(String.format("%020d", 400L) + SegmentUtils.getFileTypeSuffix(SegmentUtils.SegmentFileType.LOG))));
        assertTrue(filesAfterGc.stream().anyMatch(f -> f.getName().equals(String.format("%020d", 400L) + SegmentUtils.getFileTypeSuffix(SegmentUtils.SegmentFileType.INDEX))));
        assertTrue(filesAfterGc.stream().anyMatch(f -> f.getName().equals(String.format("%020d", 400L) + SegmentUtils.getFileTypeSuffix(SegmentUtils.SegmentFileType.TIMEINDEX))));
        assertTrue(filesAfterGc.stream().anyMatch(f -> f.getName().equals(String.format("%020d", 500L) + SegmentUtils.getFileTypeSuffix(SegmentUtils.SegmentFileType.LOG))));
        assertTrue(filesAfterGc.stream().anyMatch(f -> f.getName().equals(String.format("%020d", 500L) + SegmentUtils.getFileTypeSuffix(SegmentUtils.SegmentFileType.INDEX))));
        assertTrue(filesAfterGc.stream().anyMatch(f -> f.getName().equals(String.format("%020d", 500L) + SegmentUtils.getFileTypeSuffix(SegmentUtils.SegmentFileType.TIMEINDEX))));

        assertFalse(TopicPartitionMetadataUtil.isLocked(tp));
    }

    @Test
    void testNoMetadataFileDoesNotPerformGc() throws IOException {
        LeadershipWatcher leadershipWatcher = Mockito.mock(LeadershipWatcher.class);
        TopicPartition tp = new TopicPartition(TEST_TOPIC_A, 0);
        when(leadershipWatcher.getLeadingPartitions()).thenReturn(Collections.singleton(tp));

        mockManager = new MockSegmentManager(getSegmentUploaderConfiguration(TEST_CLUSTER), environmentProvider, endpointProvider, leadershipWatcher);
        Files.createDirectories(Paths.get(DIRECTORY.getPath(), tp.toString()));

        // write mock data
        Set<Long> offsets = Set.of(0L, 100L, 200L, 300L, 400L, 500L);
        for (long offset : offsets) {
            // write empty segments
            Files.write(Paths.get(DIRECTORY.getPath(), tp.toString(), String.format("%020d", offset) + SegmentUtils.getFileTypeSuffix(SegmentUtils.SegmentFileType.LOG)), "".getBytes(StandardCharsets.UTF_8));
            Files.write(Paths.get(DIRECTORY.getPath(), tp.toString(), String.format("%020d", offset) + SegmentUtils.getFileTypeSuffix(SegmentUtils.SegmentFileType.TIMEINDEX)), "".getBytes(StandardCharsets.UTF_8));
            Files.write(Paths.get(DIRECTORY.getPath(), tp.toString(), String.format("%020d", offset) + SegmentUtils.getFileTypeSuffix(SegmentUtils.SegmentFileType.INDEX)), "".getBytes(StandardCharsets.UTF_8));
        }
        Files.write(Paths.get(DIRECTORY.getPath(), tp.toString(), "offset.wm"), "".getBytes(StandardCharsets.UTF_8));

        File[] files = Paths.get(DIRECTORY.getPath(), tp.toString()).toFile().listFiles();
        assertEquals(offsets.size() * 3 + 1, files.length);

        // run cycle
        mockManager.runGarbageCollection();

        // check no new metadata was written
        TopicPartitionMetadata newMetadata = mockManager.getTopicPartitionMetadataFromStorage(tp);
        assertNull(newMetadata);

        Set<File> filesAfterGc = Set.of(Paths.get(DIRECTORY.getPath(), tp.toString()).toFile().listFiles());
        assertEquals(offsets.size() * 3 + 1, filesAfterGc.size());
        assertFalse(filesAfterGc.stream().anyMatch(f -> f.getName().equals("_metadata")));

        assertFalse(TopicPartitionMetadataUtil.isLocked(tp));
    }

    /**
     * Test that no expired segments are deleted when there are no expired entries in the time index.
     * @throws IOException
     */
    @Test
    void testNoExpiredSegments() throws IOException {
        LeadershipWatcher leadershipWatcher = Mockito.mock(LeadershipWatcher.class);
        TopicPartition tp = new TopicPartition(TEST_TOPIC_A, 0);
        when(leadershipWatcher.getLeadingPartitions()).thenReturn(Collections.singleton(tp));

        mockManager = new MockSegmentManager(getSegmentUploaderConfiguration(TEST_CLUSTER), environmentProvider, endpointProvider, leadershipWatcher);
        Files.createDirectories(Paths.get(DIRECTORY.getPath(), tp.toString()));

        // write mock metadata
        TopicPartitionMetadata metadata = new TopicPartitionMetadata(tp);   // retention is 3600 seconds (1 hour)
        TimeIndex timeIndex = new TimeIndex(TimeIndex.TimeIndexType.TOPIC_PARTITION);
        timeIndex.insertEntry(new TimeIndex.TimeIndexEntry(System.currentTimeMillis(), 100, 0L));
        timeIndex.insertEntry(new TimeIndex.TimeIndexEntry(System.currentTimeMillis(), 100, 100L));
        timeIndex.insertEntry(new TimeIndex.TimeIndexEntry(System.currentTimeMillis(), 100, 200L));
        timeIndex.insertEntry(new TimeIndex.TimeIndexEntry(System.currentTimeMillis(), 100, 300L));
        timeIndex.insertEntry(new TimeIndex.TimeIndexEntry(System.currentTimeMillis(), 100, 400L));
        metadata.updateMetadata(TopicPartitionMetadata.TIMEINDEX_KEY, timeIndex);
        mockManager.writeMetadataToStorage(metadata);

        // write mock data
        Set<Long> offsets = Set.of(0L, 100L, 200L, 300L, 400L, 500L);
        for (long offset : offsets) {
            // write empty segments
            Files.write(Paths.get(DIRECTORY.getPath(), tp.toString(), String.format("%020d", offset) + SegmentUtils.getFileTypeSuffix(SegmentUtils.SegmentFileType.LOG)), "".getBytes(StandardCharsets.UTF_8));
            Files.write(Paths.get(DIRECTORY.getPath(), tp.toString(), String.format("%020d", offset) + SegmentUtils.getFileTypeSuffix(SegmentUtils.SegmentFileType.TIMEINDEX)), "".getBytes(StandardCharsets.UTF_8));
            Files.write(Paths.get(DIRECTORY.getPath(), tp.toString(), String.format("%020d", offset) + SegmentUtils.getFileTypeSuffix(SegmentUtils.SegmentFileType.INDEX)), "".getBytes(StandardCharsets.UTF_8));
        }
        Files.write(Paths.get(DIRECTORY.getPath(), tp.toString(), "offset.wm"), "".getBytes(StandardCharsets.UTF_8));

        File[] files = Paths.get(DIRECTORY.getPath(), tp.toString()).toFile().listFiles();
        assertEquals(offsets.size() * 3 + 2, files.length);

        // run cycle
        mockManager.runGarbageCollection();

        // check metadata is same as before
        TopicPartitionMetadata newMetadata = mockManager.getTopicPartitionMetadataFromStorage(tp);
        assertEquals(metadata, newMetadata);

        // check segments were not deleted
        File[] newFiles = Paths.get(DIRECTORY.getPath(), tp.toString()).toFile().listFiles();
        assertEquals(offsets.size() * 3 + 2, newFiles.length);

        assertFalse(TopicPartitionMetadataUtil.isLocked(tp));
    }

    /**
     * Test that no expired segments are deleted when the metadata update fails.
     * @throws IOException
     */
    @Test
    void testMetadataUpdateFailureSkipsSegmentDeletion() throws IOException {
        LeadershipWatcher leadershipWatcher = Mockito.mock(LeadershipWatcher.class);
        TopicPartition tp = new TopicPartition(TEST_TOPIC_A, 0);
        when(leadershipWatcher.getLeadingPartitions()).thenReturn(Collections.singleton(tp));

        SegmentUploaderConfiguration config = getSegmentUploaderConfiguration(TEST_CLUSTER);

        // Use a mock that fails metadata updates
        mockManager = new MockSegmentManagerWithFailingMetadataUpdate(config, environmentProvider, endpointProvider, leadershipWatcher);
        Files.createDirectories(Paths.get(DIRECTORY.getPath(), tp.toString()));

        // write mock metadata with expired entries
        TopicPartitionMetadata metadata = new TopicPartitionMetadata(tp);   // retention is 3600 seconds (1 hour)
        TimeIndex timeIndex = new TimeIndex(TimeIndex.TimeIndexType.TOPIC_PARTITION);
        timeIndex.insertEntry(new TimeIndex.TimeIndexEntry(System.currentTimeMillis() - 7200 * 1000, 100, 0L)); // 2 hours ago (expired)
        timeIndex.insertEntry(new TimeIndex.TimeIndexEntry(System.currentTimeMillis() - 5400 * 1000, 100, 100L)); // 1.5 hours ago (expired)
        timeIndex.insertEntry(new TimeIndex.TimeIndexEntry(System.currentTimeMillis() - 1800 * 1000, 100, 200L)); // 0.5 hours ago (not expired)
        metadata.updateMetadata(TopicPartitionMetadata.TIMEINDEX_KEY, timeIndex);

        writeMetadataToLocalFile(metadata); // write actual metadata to local file

        // write mock segments that would normally be deleted
        Set<Long> offsets = Set.of(0L, 100L, 200L);
        for (long offset : offsets) {
            Files.write(Paths.get(DIRECTORY.getPath(), tp.toString(), String.format("%020d", offset) + SegmentUtils.getFileTypeSuffix(SegmentUtils.SegmentFileType.LOG)), "".getBytes(StandardCharsets.UTF_8));
            Files.write(Paths.get(DIRECTORY.getPath(), tp.toString(), String.format("%020d", offset) + SegmentUtils.getFileTypeSuffix(SegmentUtils.SegmentFileType.TIMEINDEX)), "".getBytes(StandardCharsets.UTF_8));
            Files.write(Paths.get(DIRECTORY.getPath(), tp.toString(), String.format("%020d", offset) + SegmentUtils.getFileTypeSuffix(SegmentUtils.SegmentFileType.INDEX)), "".getBytes(StandardCharsets.UTF_8));
        }

        int initialFileCount = Paths.get(DIRECTORY.getPath(), tp.toString()).toFile().listFiles().length;

        // run garbage collection - should not delete segments due to metadata update failure
        mockManager.runGarbageCollection();

        // verify segments were NOT deleted
        int finalFileCount = Paths.get(DIRECTORY.getPath(), tp.toString()).toFile().listFiles().length;
        assertEquals(initialFileCount, finalFileCount);

        assertFalse(TopicPartitionMetadataUtil.isLocked(tp));
    }

    private static class MockSegmentManager extends SegmentManager {

        public MockSegmentManager(SegmentUploaderConfiguration config, KafkaEnvironmentProvider environmentProvider, StorageServiceEndpointProvider endpointProvider, LeadershipWatcher leadershipWatcher) {
            super(config, environmentProvider, endpointProvider, leadershipWatcher);
        }

        @Override
        public void initialize() {
        }

        @Override
        public TopicPartitionMetadata getTopicPartitionMetadataFromStorage(TopicPartition topicPartition) throws IOException {
            try {
                byte[] content = Files.readAllBytes(Paths.get(DIRECTORY.getPath(), topicPartition.toString(), TopicPartitionMetadata.FILENAME));
                String jsonString = new String(content);
                return TopicPartitionMetadata.loadFromJson(jsonString);
            } catch (NoSuchFileException e) {
                return null;
            }
        }

        @Override
        public boolean writeMetadataToStorage(TopicPartitionMetadata tpMetadata) {
            return writeMetadataToLocalFile(tpMetadata);
        }

        @Override
        public Set<Long> deleteSegmentsBeforeBaseOffsetInclusive(TopicPartition topicPartition, long baseOffset) {
            File[] files = Paths.get(DIRECTORY.getPath(), topicPartition.toString()).toFile().listFiles();
            if (files == null) {
                throw new RuntimeException("No files found");
            }
            Set<Long> offsetsDeleted = new HashSet<>();
            for (File file : files) {
                Optional<Long> offset = Utils.getBaseOffsetFromFilename(file.getName());
                if (offset.isPresent() && offset.get() <= baseOffset) {
                    if (file.delete()) {
                        offsetsDeleted.add(offset.get());
                    }
                }
            }
            return offsetsDeleted;
        }
    }

    private static boolean writeMetadataToLocalFile(TopicPartitionMetadata tpMetadata) {
        String jsonString = tpMetadata.getAsJsonString();
        try {
            Files.createDirectories(Paths.get(DIRECTORY.getPath(), tpMetadata.getTopicPartition().toString()));
            Files.write(Paths.get(DIRECTORY.getPath(), tpMetadata.getTopicPartition().toString(), TopicPartitionMetadata.FILENAME), jsonString.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    private static class MockSegmentManagerWithFailingMetadataUpdate extends MockSegmentManager {

        public MockSegmentManagerWithFailingMetadataUpdate(SegmentUploaderConfiguration config, KafkaEnvironmentProvider environmentProvider, StorageServiceEndpointProvider endpointProvider, LeadershipWatcher leadershipWatcher) {
            super(config, environmentProvider, endpointProvider, leadershipWatcher);
        }

        @Override
        public boolean writeMetadataToStorage(TopicPartitionMetadata tpMetadata) {
            // Simulate metadata update failure
            return false;
        }
    }

}
