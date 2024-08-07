package software.amazon.nio.spi.s3;

import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.io.IOException;

public class TestS3SeekableByteChannel extends S3SeekableByteChannel {

    public TestS3SeekableByteChannel(S3Path s3Path, S3AsyncClient s3Client) throws IOException {
        super(s3Path, s3Client, 0);
    }
}
