package io.synapsedb.index.config;


import java.util.Objects;

/**
 * Author: Amit Tiwari
 * Date: 31/10/25
 * Simple representation of index merge policy parameters.
 */
public final class MergePolicy {

    public enum Type {
        DEFAULT,
        LOG_BYTE_SIZE,
        NO_MERGE
    }

    private final Type type;
    private final int maxMergedSegments;
    private final long maxMergedSegmentBytes;

    private MergePolicy(Type type, int maxMergedSegments, long maxMergedSegmentBytes) {
        this.type = Objects.requireNonNull(type, "type");
        if (maxMergedSegments <= 0) {
            throw new IllegalArgumentException("maxMergedSegments must be > 0");
        }
        if (maxMergedSegmentBytes <= 0L) {
            throw new IllegalArgumentException("maxMergedSegmentBytes must be > 0");
        }
        this.maxMergedSegments = maxMergedSegments;
        this.maxMergedSegmentBytes = maxMergedSegmentBytes;
    }

    public static MergePolicy defaults() {
        return new MergePolicy(Type.DEFAULT, 10, 64L * 1024 * 1024); // 64 MB
    }

    public static MergePolicy logByteSizePolicy(int maxMergedSegments, long maxMergedSegmentBytes) {
        return new MergePolicy(Type.LOG_BYTE_SIZE, maxMergedSegments, maxMergedSegmentBytes);
    }

    public static MergePolicy noMerge() {
        return new MergePolicy(Type.NO_MERGE, Integer.MAX_VALUE, Long.MAX_VALUE);
    }

    public Type getType() {
        return type;
    }

    public int getMaxMergedSegments() {
        return maxMergedSegments;
    }

    public long getMaxMergedSegmentBytes() {
        return maxMergedSegmentBytes;
    }

    @Override
    public String toString() {
        return "MergePolicy{" +
                "type=" + type +
                ", maxMergedSegments=" + maxMergedSegments +
                ", maxMergedSegmentBytes=" + maxMergedSegmentBytes +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MergePolicy that)) return false;
        return maxMergedSegments == that.maxMergedSegments &&
                maxMergedSegmentBytes == that.maxMergedSegmentBytes &&
                type == that.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, maxMergedSegments, maxMergedSegmentBytes);
    }
}