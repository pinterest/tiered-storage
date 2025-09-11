package com.pinterest.kafka.tieredstorage.uploader.dlq;

import com.pinterest.kafka.tieredstorage.common.SegmentUtils;
import com.pinterest.kafka.tieredstorage.uploader.DirectoryTreeWatcher;
import com.pinterest.kafka.tieredstorage.uploader.SegmentUploaderConfiguration;
import com.pinterest.kafka.tieredstorage.uploader.TestBase;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestS3LocalExecutableDeadLetterQueueHandler extends TestBase {

    /**
     * Test the handler by having it send many tasks concurrently to a local executable file.
     */
    @Test
    void testConcurrentSend() throws IOException, ExecutionException, InterruptedException {
        String filePath = "/tmp/s3-local-executable-dlq-handler-test.sh";
        String segmentDirectory = "/test/path";
        List<Integer> partitions = Arrays.asList(1,2,3);
        List<String> topics = Arrays.asList("topic1", "topic2", "topic3");
        int numTasksPerPartition = 300; // should be a multiple of 3
        Map<String, List<DirectoryTreeWatcher.UploadTask>> topicToTasks = new HashMap<>();

        for (int topicIdx = 0; topicIdx < topics.size(); topicIdx++) {
            String topic = topics.get(topicIdx);
            int numPartitions = partitions.get(topicIdx);
            for (int partitionIdx = 0; partitionIdx < numPartitions; partitionIdx++) {
                List<DirectoryTreeWatcher.UploadTask> tasks = new ArrayList<>();
                for (int i = 0; i < numTasksPerPartition; i++) {
                    TopicPartition tp = new TopicPartition(topic, partitionIdx);
                    String extension;
                    if (i % 3 == 0)
                        extension = SegmentUtils.getFileTypeSuffix(SegmentUtils.SegmentFileType.LOG);
                    else if (i % 3 == 1)
                        extension = SegmentUtils.getFileTypeSuffix(SegmentUtils.SegmentFileType.INDEX);
                    else
                        extension = SegmentUtils.getFileTypeSuffix(SegmentUtils.SegmentFileType.TIMEINDEX);
                    String offset = String.format("%020d", i);
                    DirectoryTreeWatcher.UploadTask uploadTask = new DirectoryTreeWatcher.UploadTask(
                            tp,
                            offset,
                            offset + extension,
                            Paths.get(String.format("%s/%s/%s%s", segmentDirectory, tp, offset, extension))
                    );
                    uploadTask.setUploadDestinationPathString(String.format("s3://%s/%s/%s/%s%s",
                            "test-bucket", TEST_CLUSTER, tp, offset, extension));
                    tasks.add(uploadTask);
                }
                topicToTasks.computeIfAbsent(topic, k -> new ArrayList<>()).addAll(tasks);
            }
        }
        int numExpectedTasks = numTasksPerPartition * partitions.stream().mapToInt(Integer::intValue).sum();
        assertEquals(numExpectedTasks, topicToTasks.values().stream().mapToInt(List::size).sum());

        SegmentUploaderConfiguration config = getSegmentUploaderConfiguration(TEST_CLUSTER);
        setProperty(config, S3LocalExecutableDeadLetterQueueHandler.PATH_CONFIG_KEY, filePath);

        S3LocalExecutableDeadLetterQueueHandler handler = new S3LocalExecutableDeadLetterQueueHandler(config);

        // send all tasks in 3 parallel threads - 1 for each topic - to test concurrent writes
        Thread thread0 = new Thread(() -> {
            for (DirectoryTreeWatcher.UploadTask task : topicToTasks.get(topics.get(0))) {
                handler.send(task, new NoSuchFileException("test exception " + task.getTopicPartition() + ": " + task.getFullFilename()));
            }
        });
        Thread thread1 = new Thread(() -> {
            for (DirectoryTreeWatcher.UploadTask task : topicToTasks.get(topics.get(1))) {
                handler.send(task, new NoSuchFileException("test exception " + task.getTopicPartition() + ": " + task.getFullFilename()));
            }
        });
        Thread thread2 = new Thread(() -> {
            for (DirectoryTreeWatcher.UploadTask task : topicToTasks.get(topics.get(2))) {
                handler.send(task, new NoSuchFileException("test exception " + task.getTopicPartition() + ": " + task.getFullFilename()));
            }
        });

        thread0.start();
        thread1.start();
        thread2.start();

        thread0.join();
        thread1.join();
        thread2.join();

        // check the file content
        int numLinesPerTask = 6;    // this behavior is hardcoded in the handler
        Path path = Paths.get(filePath);
        List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        assertEquals(numLinesPerTask * numExpectedTasks, lines.size());   // 6 lines per task is logged

        String expectedTopicPartition = null;
        String expectedFilename = null;
        for (int lineIdx = 0; lineIdx < lines.size(); lineIdx++) {
            String line = lines.get(lineIdx);
            int mod = lineIdx % numLinesPerTask;
            switch (mod) {
                case 0:
                    assertTrue(line.startsWith("# timestamp: "));
                    break;
                case 1:
                    assertTrue(line.startsWith("# human_timestamp: "));
                    break;
                case 2:
                    assertTrue(line.equals("# exception: " + NoSuchFileException.class.getName()));
                    break;
                case 3:
                    String prefix = "# message: test exception ";
                    assertTrue(line.startsWith(prefix));
                    String remainder = line.substring(prefix.length());
                    String[] parts = remainder.split(": ");
                    assertEquals(2, parts.length);
                    expectedTopicPartition = parts[0];
                    expectedFilename = parts[1];
                    break;
                case 4:
                    assertNotNull(expectedTopicPartition);
                    assertNotNull(expectedFilename);
                    assertEquals(
                            String.format("aws s3 cp %s/%s/%s s3://%s/%s/%s/%s",
                                segmentDirectory,
                                expectedTopicPartition,
                                expectedFilename,
                                "test-bucket",
                                TEST_CLUSTER,
                                expectedTopicPartition,
                                expectedFilename
                            ),
                            line
                    );
                    break;
                case 5:
                    assertTrue(line.startsWith("################"));
                    expectedTopicPartition = null;
                    expectedFilename = null;
                    break;
            }
        }
        Files.delete(path);
    }
}
