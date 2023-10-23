package com.automq.stream.s3.operator;

import com.automq.stream.s3.DirectByteBufAlloc;
import com.automq.stream.s3.compact.AsyncTokenBucketThrottle;
import com.automq.stream.utils.FutureUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;

import java.util.concurrent.CompletableFuture;

/**
 * If object data size is less than ObjectWriter.MAX_UPLOAD_SIZE, we should use single upload to upload it.
 * Else, we should use multi-part upload to upload it.
 */
class ProxyWriter implements Writer {
    private final S3Operator operator;
    private final String path;
    private final long minPartSize;
    private final AsyncTokenBucketThrottle readThrottle;
    final ObjectWriter objectWriter = new ObjectWriter();
    Writer multiPartWriter = null;

    public ProxyWriter(S3Operator operator, String path, long minPartSize, AsyncTokenBucketThrottle readThrottle) {
        this.operator = operator;
        this.path = path;
        this.minPartSize = minPartSize;
        this.readThrottle = readThrottle;
    }

    public ProxyWriter(S3Operator operator, String path, AsyncTokenBucketThrottle readThrottle) {
        this(operator, path, MIN_PART_SIZE, readThrottle);
    }

    @Override
    public CompletableFuture<Void> write(ByteBuf part) {
        if (multiPartWriter != null) {
            return multiPartWriter.write(part);
        } else {
            if (objectWriter.isFull()) {
                newMultiPartWriter();
                return multiPartWriter.write(part);
            } else {
                return objectWriter.write(part);
            }
        }
    }

    @Override
    public void copyWrite(String sourcePath, long start, long end) {
        if (multiPartWriter == null) {
            newMultiPartWriter();
        }
        multiPartWriter.copyWrite(sourcePath, start, end);
    }

    @Override
    public boolean hasBatchingPart() {
        if (multiPartWriter != null) {
            return multiPartWriter.hasBatchingPart();
        } else {
            return objectWriter.hasBatchingPart();
        }
    }

    @Override
    public CompletableFuture<Void> close() {
        if (multiPartWriter != null) {
            return multiPartWriter.close();
        } else {
            return objectWriter.close();
        }
    }

    private void newMultiPartWriter() {
        this.multiPartWriter = new MultiPartWriter(operator, path, minPartSize, readThrottle);
        if (objectWriter.data.readableBytes() > 0) {
            FutureUtil.propagate(multiPartWriter.write(objectWriter.data), objectWriter.cf);
        } else {
            objectWriter.cf.complete(null);
        }
    }

    class ObjectWriter implements Writer {
        // max upload size, when object data size is larger MAX_UPLOAD_SIZE, we should use multi-part upload to upload it.
        static final long MAX_UPLOAD_SIZE = 32L * 1024 * 1024;
        CompletableFuture<Void> cf = new CompletableFuture<>();
        CompositeByteBuf data = DirectByteBufAlloc.compositeByteBuffer();

        @Override
        public CompletableFuture<Void> write(ByteBuf part) {
            data.addComponent(true, part);
            return cf;
        }

        @Override
        public void copyWrite(String sourcePath, long start, long end) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasBatchingPart() {
            return true;
        }

        @Override
        public CompletableFuture<Void> close() {
            FutureUtil.propagate(operator.write(path, data), cf);
            cf.whenComplete((nil, ex) -> data.release());
            return cf;
        }

        public boolean isFull() {
            return data.readableBytes() > MAX_UPLOAD_SIZE;
        }
    }
}
