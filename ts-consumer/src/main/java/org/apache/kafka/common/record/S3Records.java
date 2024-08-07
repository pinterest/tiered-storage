package org.apache.kafka.common.record;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.Time;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.nio.spi.s3.S3FileSystem;
import software.amazon.nio.spi.s3.S3FileSystemProvider;
import software.amazon.nio.spi.s3.S3Path;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

public class S3Records extends AbstractRecords {
    private final String bucket;
    private final String key;
    private final SeekableByteChannel channel;
    private final Iterable<S3ChannelRecordBatch> batches;
    private final AtomicInteger size;
    private final boolean isSlice;
    private final int start;
    private final int end;
    private static final S3FileSystemProvider s3FileSystemProvider = new S3FileSystemProvider();


    public S3Records(String bucket,
                     String key,
                     SeekableByteChannel channel,
                     int start,
                     int end,
                     boolean isSlice) throws IOException {
        this.bucket = bucket;
        this.key = key;
        this.channel = channel;
        this.start = start;
        this.end = end;
        this.isSlice = isSlice;
        this.size = new AtomicInteger();

        if (isSlice) {
            // don't check the file size if this is just a slice view
            size.set(end - start);
        } else {
            if (channel.size() > Integer.MAX_VALUE)
                throw new KafkaException("The size of segment s3://" + bucket + "/" + key + " (" + channel.size() +
                        ") is larger than the maximum allowed segment size of " + Integer.MAX_VALUE);

            int limit = Math.min((int) channel.size(), end);
            size.set(limit - start);

            // if this is not a slice, update the file pointer to the end of the file
            // set the file position to the last byte in the file
            channel.position(limit);
        }

        batches = batchesFrom(start);
    }

    public Iterable<S3ChannelRecordBatch> batchesFrom(final int start) {
        return () -> batchIterator(start);
    }

    @Override
    public AbstractIterator<S3ChannelRecordBatch> batchIterator() {
        return batchIterator(start);
    }

    private AbstractIterator<S3ChannelRecordBatch> batchIterator(int start) {
        final int end;
        if (isSlice)
            end = this.end;
        else
            end = this.sizeInBytes();
        S3LogInputStream inputStream = new S3LogInputStream(this, start, end);
        return new RecordBatchIterator<>(inputStream);
    }

    public static S3FileSystemProvider getS3FileSystemProvider() {
        return s3FileSystemProvider;
    }

    public static S3Records open(String bucket,
                                 String key,
                                 int startPosition,
                                 boolean mutable,
                                 boolean fileAlreadyExists,
                                 int initFileSize,
                                 boolean preallocate) throws IOException {
        URI uri = URI.create("s3://" + bucket);
        S3Object s3Object = S3Object.builder().key(key).build();
        S3FileSystem fileSystem = getS3FileSystemProvider().getFileSystem(uri);
        S3Path s3Path = S3Path.getPath(getS3FileSystemProvider().getFileSystem(uri), s3Object);

        SeekableByteChannel channel = fileSystem.provider().newByteChannel(s3Path, Collections.singleton(StandardOpenOption.READ));
        int end = (!fileAlreadyExists && preallocate) ? startPosition : initFileSize;
        return new S3Records(bucket, key, channel, startPosition, end, startPosition > 0);
    }

    public SeekableByteChannel channel() {
        return channel;
    }

    public String getBucket() {
        return bucket;
    }

    public String getKey() {
        return key;
    }

    @Override
    public long writeTo(GatheringByteChannel channel, long position, int length) {

        throw new NotImplementedException("S3Records objects are meant to be used only for reading records from S3");
    }

    @Override
    public Iterable<? extends RecordBatch> batches() {
        return batches;
    }

    @Override
    public ConvertedRecords<? extends Records> downConvert(byte toMagic, long firstOffset, Time time) {
        return null;
    }

    @Override
    public int sizeInBytes() {
        return size.get();
    }

    public void close() throws IOException {
        this.channel.close();
    }
}
