package io.synapsedb.core.durability;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Manages a single segment of the Write-Ahead Log.
 * Each segment is a fixed-size file that stores log entries sequentially.
 * When a segment fills up, a new one is created.
 *
 * @author Amit Tiwari
 */
public class LogSegment implements Closeable {

    private static final int HEADER_SIZE = 16; // Magic(4) + Version(4) + SegmentId(8)
    private static final int MAGIC_NUMBER = 0x53594E41; // "SYNA" in hex
    private static final int VERSION = 1;

    private final Path segmentPath;
    private final long segmentId;
    private final long maxSegmentSize;
    private final FileChannel channel;
    private final Lock writeLock;

    private final AtomicLong currentSize;
    private final AtomicLong entryCount;
    private volatile boolean closed = false;

    public LogSegment(Path segmentPath, long segmentId, long maxSegmentSize) throws IOException {
        this.segmentPath = segmentPath;
        this.segmentId = segmentId;
        this.maxSegmentSize = maxSegmentSize;
        this.writeLock = new ReentrantLock();
        this.currentSize = new AtomicLong(0);
        this.entryCount = new AtomicLong(0);

        // Create or open the segment file
        this.channel = FileChannel.open(segmentPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.SYNC); // Force sync for durability

        // If new file, write header
        if (channel.size() == 0) {
            writeHeader();
        } else {
            // Existing file - validate and set size
            validateHeader();
            currentSize.set(channel.size());
        }
    }

    /**
     * Write header to new segment file
     */
    private void writeHeader() throws IOException {
        ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
        header.putInt(MAGIC_NUMBER);
        header.putInt(VERSION);
        header.putLong(segmentId);
        header.flip();

        channel.write(header, 0);
        channel.force(true); // Ensure header is written to disk
        currentSize.set(HEADER_SIZE);
    }

    /**
     * Validate segment header
     */
    private void validateHeader() throws IOException {
        ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
        int read = channel.read(header, 0);

        if (read < HEADER_SIZE) {
            throw new IOException("Invalid segment file: header too short");
        }

        header.flip();
        int magic = header.getInt();
        int version = header.getInt();
        long storedSegmentId = header.getLong();

        if (magic != MAGIC_NUMBER) {
            throw new IOException("Invalid segment file: bad magic number");
        }
        if (version != VERSION) {
            throw new IOException("Invalid segment file: unsupported version " + version);
        }
        if (storedSegmentId != segmentId) {
            throw new IOException("Segment ID mismatch: expected " + segmentId + ", got " + storedSegmentId);
        }
    }

    /**
     * Append a log entry to this segment.
     * Returns false if segment is full.
     */
    public boolean append(LogEntry entry) throws IOException {
        if (closed) {
            throw new IOException("Cannot append to closed segment");
        }

        byte[] entryBytes = entry.toBytes();
        int entrySize = entryBytes.length;

        // Check if we have space (including 4 bytes for length prefix)
        if (currentSize.get() + entrySize + 4 > maxSegmentSize) {
            return false; // Segment full
        }

        writeLock.lock();
        try {
            // Write: [length(4)][entry data]
            ByteBuffer buffer = ByteBuffer.allocate(4 + entrySize);
            buffer.putInt(entrySize);
            buffer.put(entryBytes);
            buffer.flip();

            long position = currentSize.get();
            int written = channel.write(buffer, position);

            if (written != buffer.capacity()) {
                throw new IOException("Failed to write complete log entry");
            }

            // Force to disk for durability
            channel.force(true);

            currentSize.addAndGet(written);
            entryCount.incrementAndGet();

            return true;

        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Read all entries from this segment
     */
    public java.util.List<LogEntry> readAllEntries() throws IOException {
        java.util.List<LogEntry> entries = new java.util.ArrayList<>();
        long position = HEADER_SIZE;

        while (position < currentSize.get()) {
            // Read entry length
            ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
            int read = channel.read(lengthBuffer, position);

            if (read < 4) {
                break; // End of valid entries
            }

            lengthBuffer.flip();
            int entrySize = lengthBuffer.getInt();

            if (entrySize <= 0 || entrySize > 10 * 1024 * 1024) { // Max 10MB per entry
                throw new IOException("Invalid entry size: " + entrySize);
            }

            // Read entry data
            ByteBuffer entryBuffer = ByteBuffer.allocate(entrySize);
            read = channel.read(entryBuffer, position + 4);

            if (read < entrySize) {
                // Incomplete entry - may be corrupted
                break;
            }

            entryBuffer.flip();
            byte[] entryBytes = new byte[entrySize];
            entryBuffer.get(entryBytes);

            try {
                LogEntry entry = LogEntry.fromBytes(entryBytes);
                if (entry.isValid()) {
                    entries.add(entry);
                } else {
                    // Corrupted entry - stop reading
                    break;
                }
            } catch (Exception e) {
                // Failed to parse entry - stop reading
                break;
            }

            position += 4 + entrySize;
        }

        return entries;
    }

    /**
     * Check if this segment is full
     */
    public boolean isFull() {
        return currentSize.get() >= maxSegmentSize;
    }

    /**
     * Sync this segment to disk
     */
    public void sync() throws IOException {
        if (!closed) {
            channel.force(true);
        }
    }

    /**
     * Delete this segment file
     */
    public void delete() throws IOException {
        close();
        Files.deleteIfExists(segmentPath);
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            channel.close();
        }
    }

    // Getters

    public long getSegmentId() {
        return segmentId;
    }

    public Path getSegmentPath() {
        return segmentPath;
    }

    public long getCurrentSize() {
        return currentSize.get();
    }

    public long getEntryCount() {
        return entryCount.get();
    }

    public boolean isClosed() {
        return closed;
    }

    @Override
    public String toString() {
        return String.format("LogSegment{id=%d, size=%d/%d, entries=%d, path=%s}",
                segmentId, currentSize.get(), maxSegmentSize, entryCount.get(), segmentPath.getFileName());
    }
}

