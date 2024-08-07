package org.apache.kafka.common.record;

import org.apache.kafka.common.errors.CorruptRecordException;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

import static org.apache.kafka.common.record.Records.HEADER_SIZE_UP_TO_MAGIC;
import static org.apache.kafka.common.record.Records.LOG_OVERHEAD;
import static org.apache.kafka.common.record.Records.MAGIC_OFFSET;
import static org.apache.kafka.common.record.Records.OFFSET_OFFSET;
import static org.apache.kafka.common.record.Records.SIZE_OFFSET;

public class S3LogInputStream implements LogInputStream<S3ChannelRecordBatch> {
    private int position;
    private final int end;
    private final S3Records s3Records;
    private final ByteBuffer logHeaderBuffer = ByteBuffer.allocate(HEADER_SIZE_UP_TO_MAGIC);

    S3LogInputStream(S3Records s3Records, int start, int end) {
        this.s3Records = s3Records;
        this.position = start;
        this.end = end;
    }

    @Override
    public S3ChannelRecordBatch nextBatch() throws IOException {
        SeekableByteChannel channel = s3Records.channel();
        if (position >= end - HEADER_SIZE_UP_TO_MAGIC)
            return null;

        logHeaderBuffer.rewind();
        readFullyOrFail(channel, logHeaderBuffer, position, "log header");

        logHeaderBuffer.rewind();
        long offset = logHeaderBuffer.getLong(OFFSET_OFFSET);
        int size = logHeaderBuffer.getInt(SIZE_OFFSET);

        // V0 has the smallest overhead, stricter checking is done later
        if (size < LegacyRecord.RECORD_OVERHEAD_V0)
            throw new CorruptRecordException(String.format("Found record size %d smaller than minimum record " +
                    "overhead (%d) in %s://%s.", size, LegacyRecord.RECORD_OVERHEAD_V0, s3Records.getBucket(), s3Records.getKey()));

        if (position > end - LOG_OVERHEAD - size)
            return null;

        byte magic = logHeaderBuffer.get(MAGIC_OFFSET);
        final S3ChannelRecordBatch batch;

        batch = new DefaultS3ChannelRecordBatch(offset, magic, s3Records, position, size);

        position += batch.sizeInBytes();
        return batch;
    }

    protected static void readFullyOrFail(SeekableByteChannel channel, ByteBuffer destinationBuffer, int position, String description) throws IOException {
        if (position < 0) {
            throw new IllegalArgumentException("The file channel position cannot be negative, but it is " + position);
        }
        int expectedReadBytes = destinationBuffer.remaining();
        readFully(channel, destinationBuffer, position);
        if (destinationBuffer.hasRemaining()) {
            throw new EOFException(String.format("Failed to read `%s` from file channel `%s`. Expected to read %d bytes, " +
                            "but reached end of file after reading %d bytes. Started read from position %d.",
                    description, channel, expectedReadBytes, expectedReadBytes - destinationBuffer.remaining(), position));
        }
    }

    protected static void readFully(SeekableByteChannel channel, ByteBuffer destinationBuffer, int position) throws IOException {
        if (position < 0) {
            throw new IllegalArgumentException("The file channel position cannot be negative, but it is " + position);
        }
        long currentPosition = position;
        int bytesRead;
        do {
            channel.position(currentPosition);
            bytesRead = channel.read(destinationBuffer);
            currentPosition += bytesRead;
        } while (bytesRead != -1 && destinationBuffer.hasRemaining());
    }

}
