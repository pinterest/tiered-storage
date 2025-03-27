package org.apache.kafka.common.record;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.CloseableIterator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.Iterator;
import java.util.Objects;

import static org.apache.kafka.common.record.Records.LOG_OVERHEAD;

/**
 * Log entry backed by an underlying S3Channel. This allows iteration over the record batches
 * without needing to read the record data into memory until it is needed. The downside
 * is that entries will generally no longer be readable when the underlying channel is closed.
 */
public abstract class S3ChannelRecordBatch extends AbstractRecordBatch {
    protected final long offset;
    protected final byte magic;
    protected final S3Records s3Records;
    protected final int position;
    protected final int batchSize;

    private RecordBatch fullBatch;
    private RecordBatch batchHeader;

    S3ChannelRecordBatch(long offset,
                         byte magic,
                         S3Records s3Records,
                         int position,
                         int batchSize) {
        this.offset = offset;
        this.magic = magic;
        this.s3Records = s3Records;
        this.position = position;
        this.batchSize = batchSize;
    }

    @Override
    public CompressionType compressionType() {
        return loadBatchHeader().compressionType();
    }

    @Override
    public TimestampType timestampType() {
        return loadBatchHeader().timestampType();
    }

    @Override
    public long checksum() {
        return loadBatchHeader().checksum();
    }

    @Override
    public long maxTimestamp() {
        return loadBatchHeader().maxTimestamp();
    }

    public int position() {
        return position;
    }

    @Override
    public byte magic() {
        return magic;
    }

    @Override
    public Iterator<Record> iterator() {
        return loadFullBatch().iterator();
    }

    @Override
    public CloseableIterator<Record> streamingIterator(BufferSupplier bufferSupplier) {
        return loadFullBatch().streamingIterator(bufferSupplier);
    }

    @Override
    public boolean isValid() {
        return loadFullBatch().isValid();
    }

    @Override
    public void ensureValid() {
        loadFullBatch().ensureValid();
    }

    @Override
    public int sizeInBytes() {
        return LOG_OVERHEAD + batchSize;
    }

    @Override
    public void writeTo(ByteBuffer buffer) {
        SeekableByteChannel channel = s3Records.channel();
        try {
            int limit = buffer.limit();
            buffer.limit(buffer.position() + sizeInBytes());
            S3LogInputStream.readFully(channel, buffer, position);
            buffer.limit(limit);
        } catch (IOException e) {
            throw new KafkaException("Failed to read record batch at position " + position + " from " + s3Records, e);
        }
    }

    protected abstract RecordBatch toMemoryRecordBatch(ByteBuffer buffer);

    protected abstract int headerSize();

    protected RecordBatch loadFullBatch() {
        if (fullBatch == null) {
            batchHeader = null;
            fullBatch = loadBatchWithSize(sizeInBytes(), "full record batch");
        }
        return fullBatch;
    }

    protected RecordBatch loadBatchHeader() {
        if (fullBatch != null)
            return fullBatch;

        if (batchHeader == null)
            batchHeader = loadBatchWithSize(headerSize(), "record batch header");

        return batchHeader;
    }

    private RecordBatch loadBatchWithSize(int size, String description) {
        SeekableByteChannel channel = s3Records.channel();
        try {
            ByteBuffer buffer = ByteBuffer.allocate(size);
            S3LogInputStream.readFullyOrFail(channel, buffer, position, description);
            buffer.rewind();
            return toMemoryRecordBatch(buffer);
        } catch (IOException e) {
            throw new KafkaException("Failed to load record batch at position " + position + " from " + s3Records, e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        S3ChannelRecordBatch that = (S3ChannelRecordBatch) o;

        SeekableByteChannel channel = s3Records == null ? null : s3Records.channel();
        SeekableByteChannel thatChannel = that.s3Records == null ? null : that.s3Records.channel();

        return offset == that.offset &&
                position == that.position &&
                batchSize == that.batchSize &&
                Objects.equals(channel, thatChannel);
    }

    @Override
    public int hashCode() {
        SeekableByteChannel channel = s3Records == null ? null : s3Records.channel();

        int result = Long.hashCode(offset);
        result = 31 * result + (channel != null ? channel.hashCode() : 0);
        result = 31 * result + position;
        result = 31 * result + batchSize;
        return result;
    }

    @Override
    public String toString() {
        return "S3ChannelRecordBatch(magic: " + magic +
                ", offset: " + offset +
                ", size: " + batchSize + ")";
    }
}
