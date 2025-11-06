package io.synapsedb.durability;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.time.Instant;

/**
 * Represents a single entry in the Write-Ahead Log (WAL).
 * Each entry contains an operation that can be replayed during recovery.
 *
 * @author Amit Tiwari
 */
public class LogEntry implements Serializable {
    private static final long serialVersionUID = 1L;

    private final long sequenceNumber;
    private final OperationType operationType;
    private final String indexName;
    private final long timestamp;
    private final byte[] data;
    private final int checksum;

    /**
     * Types of operations that can be logged
     */
    public enum OperationType {
        ADD_DOCUMENT((byte) 1),
        UPDATE_DOCUMENT((byte) 2),
        DELETE_DOCUMENT((byte) 3),
        COMMIT((byte) 4),
        FLUSH((byte) 5),
        CHECKPOINT((byte) 6);

        private final byte code;

        OperationType(byte code) {
            this.code = code;
        }

        public byte getCode() {
            return code;
        }

        public static OperationType fromCode(byte code) {
            for (OperationType type : values()) {
                if (type.code == code) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Unknown operation type code: " + code);
        }
    }

    public LogEntry(long sequenceNumber, OperationType operationType, String indexName, byte[] data) {
        this(sequenceNumber, operationType, indexName, Instant.now().toEpochMilli(), data);
    }

    /**
     * Constructor used for deserialization with explicit timestamp
     */
    private LogEntry(long sequenceNumber, OperationType operationType, String indexName, long timestamp, byte[] data) {
        this.sequenceNumber = sequenceNumber;
        this.operationType = operationType;
        this.indexName = indexName;
        this.timestamp = timestamp;
        this.data = data;
        this.checksum = calculateChecksum();
    }

    /**
     * Serialize this log entry to bytes for writing to disk
     */
    public byte[] toBytes() {
        // Format: [seqNum(8)][opType(1)][indexNameLen(4)][indexName(var)][timestamp(8)][dataLen(4)][data(var)][checksum(4)]
        int indexNameBytes = indexName.getBytes().length;
        int totalSize = 8 + 1 + 4 + indexNameBytes + 8 + 4 + data.length + 4;

        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.putLong(sequenceNumber);
        buffer.put(operationType.getCode());
        buffer.putInt(indexNameBytes);
        buffer.put(indexName.getBytes());
        buffer.putLong(timestamp);
        buffer.putInt(data.length);
        buffer.put(data);
        buffer.putInt(checksum);

        return buffer.array();
    }

    /**
     * Deserialize a log entry from bytes
     */
    public static LogEntry fromBytes(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        long sequenceNumber = buffer.getLong();
        byte opCode = buffer.get();
        OperationType operationType = OperationType.fromCode(opCode);

        int indexNameLen = buffer.getInt();
        byte[] indexNameBytes = new byte[indexNameLen];
        buffer.get(indexNameBytes);
        String indexName = new String(indexNameBytes);

        long timestamp = buffer.getLong();

        int dataLen = buffer.getInt();
        byte[] data = new byte[dataLen];
        buffer.get(data);

        int storedChecksum = buffer.getInt();

        // Use private constructor that preserves timestamp
        LogEntry entry = new LogEntry(sequenceNumber, operationType, indexName, timestamp, data);
        if (entry.checksum != storedChecksum) {
            throw new IllegalStateException("Checksum mismatch - log entry may be corrupted");
        }

        return entry;
    }

    /**
     * Calculate CRC32 checksum for data integrity verification
     */
    private int calculateChecksum() {
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        crc.update(ByteBuffer.allocate(8).putLong(sequenceNumber).array());
        crc.update(operationType.getCode());
        crc.update(indexName.getBytes());
        crc.update(ByteBuffer.allocate(8).putLong(timestamp).array());
        crc.update(data);
        return (int) crc.getValue();
    }

    /**
     * Verify the integrity of this log entry
     */
    public boolean isValid() {
        return calculateChecksum() == checksum;
    }

    // Getters

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public OperationType getOperationType() {
        return operationType;
    }

    public String getIndexName() {
        return indexName;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public byte[] getData() {
        return data;
    }

    public int getChecksum() {
        return checksum;
    }

    public int getSize() {
        return toBytes().length;
    }

    @Override
    public String toString() {
        return String.format("LogEntry{seq=%d, type=%s, index='%s', timestamp=%d, dataSize=%d, valid=%s}",
                sequenceNumber, operationType, indexName, timestamp, data.length, isValid());
    }
}

