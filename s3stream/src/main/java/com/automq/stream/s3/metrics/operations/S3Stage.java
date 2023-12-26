package com.automq.stream.s3.metrics.operations;

public enum S3Stage {

    /* Append WAL stages start */
    APPEND_WAL_BEFORE(S3Operation.APPEND_STORAGE_WAL, "before"),
    APPEND_WAL_BLOCK_POLLED(S3Operation.APPEND_STORAGE_WAL, "block_polled"),
    APPEND_WAL_AWAIT(S3Operation.APPEND_STORAGE_WAL, "await"),
    APPEND_WAL_WRITE(S3Operation.APPEND_STORAGE_WAL, "write"),
    APPEND_WAL_AFTER(S3Operation.APPEND_STORAGE_WAL, "after"),
    APPEND_WAL_COMPLETE(S3Operation.APPEND_STORAGE_WAL, "complete"),
    /* Append WAL stages end */

    /* Force upload WAL start */
    FORCE_UPLOAD_WAL_AWAIT_INFLIGHT(S3Operation.FORCE_UPLOAD_STORAGE_WAL, "await_inflight"),
    FORCE_UPLOAD_WAL_COMPLETE(S3Operation.FORCE_UPLOAD_STORAGE_WAL, "complete");
    /* Force upload WAL end */


    private final S3Operation operation;
    private final String name;

    S3Stage(S3Operation operation, String name) {
        this.operation = operation;
        this.name = name;
    }

    public S3Operation getOperation() {
        return operation;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "S3Stage{" +
                "operation=" + operation.getName() +
                ", name='" + name + '\'' +
                '}';
    }
}
