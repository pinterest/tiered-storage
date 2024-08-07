package org.apache.kafka.common.record;

import java.nio.ByteBuffer;

class DefaultS3ChannelRecordBatch extends S3ChannelRecordBatch {

    DefaultS3ChannelRecordBatch(long offset,
                                byte magic,
                                S3Records s3Records,
                                int position,
                                int batchSize) {
        super(offset, magic, s3Records, position, batchSize);
    }

    @Override
    protected RecordBatch toMemoryRecordBatch(ByteBuffer buffer) {
        return new DefaultRecordBatch(buffer);
    }

    @Override
    public long baseOffset() {
        return offset;
    }

    @Override
    public long lastOffset() {
        return loadBatchHeader().lastOffset();
    }

    @Override
    public long producerId() {
        return loadBatchHeader().producerId();
    }

    @Override
    public short producerEpoch() {
        return loadBatchHeader().producerEpoch();
    }

    @Override
    public int baseSequence() {
        return loadBatchHeader().baseSequence();
    }

    @Override
    public int lastSequence() {
        return loadBatchHeader().lastSequence();
    }

    @Override
    public Integer countOrNull() {
        return loadBatchHeader().countOrNull();
    }

    @Override
    public boolean isTransactional() {
        return loadBatchHeader().isTransactional();
    }

    @Override
    public boolean isControlBatch() {
        return loadBatchHeader().isControlBatch();
    }

    @Override
    public int partitionLeaderEpoch() {
        return loadBatchHeader().partitionLeaderEpoch();
    }

    @Override
    protected int headerSize() {
        return DefaultRecordBatch.RECORD_BATCH_OVERHEAD;
    }
}
