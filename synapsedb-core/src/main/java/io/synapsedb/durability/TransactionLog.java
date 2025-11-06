package io.synapsedb.durability;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Write-Ahead Log (WAL) implementation for SynapseDB.
 * Ensures durability by logging all operations before they're applied to the index.
 *
 * Key features:
 * - Sequential writes for performance
 * - Segment-based storage for easy cleanup
 * - Checksum validation for integrity
 * - Crash recovery support
 *
 * @author Amit Tiwari
 */
public class TransactionLog implements Closeable {

    private static final long DEFAULT_MAX_SEGMENT_SIZE = 64 * 1024 * 1024; // 64MB per segment
    private static final int MAX_SEGMENTS_TO_KEEP = 10; // Keep last 10 segments after checkpoint

    private final Path logDirectory;
    private final long maxSegmentSize;
    private final AtomicLong sequenceNumber;
    private final ReadWriteLock lock;
    private final Map<Long, LogSegment> segments;

    private volatile LogSegment currentSegment;
    private volatile boolean closed = false;
    private volatile long lastCheckpointSeq = 0;

    public TransactionLog(Path baseDirectory) throws IOException {
        this(baseDirectory, DEFAULT_MAX_SEGMENT_SIZE);
    }

    public TransactionLog(Path baseDirectory, long maxSegmentSize) throws IOException {
        this.logDirectory = baseDirectory.resolve("wal");
        this.maxSegmentSize = maxSegmentSize;
        this.sequenceNumber = new AtomicLong(0);
        this.lock = new ReentrantReadWriteLock();
        this.segments = new ConcurrentHashMap<>();

        // Create log directory if needed
        Files.createDirectories(logDirectory);

        // Discover and load existing segments
        discoverSegments();

        // Create initial segment if none exist
        if (currentSegment == null) {
            createNewSegment();
        }
    }

    /**
     * Discover existing log segments on startup
     */
    private void discoverSegments() throws IOException {
        List<Path> segmentFiles;
        try (var stream = Files.list(logDirectory)) {
            segmentFiles = stream
                    .filter(p -> p.getFileName().toString().endsWith(".log"))
                    .sorted()
                    .toList();
        }

        long maxSeq = 0;
        LogSegment latestSegment = null;

        for (Path segmentPath : segmentFiles) {
            try {
                long segmentId = extractSegmentId(segmentPath);
                LogSegment segment = new LogSegment(segmentPath, segmentId, maxSegmentSize);
                segments.put(segmentId, segment);

                // Track the latest segment
                if (segmentId > maxSeq) {
                    maxSeq = segmentId;
                    latestSegment = segment;
                }

                // Read entries to find max sequence number
                List<LogEntry> entries = segment.readAllEntries();
                for (LogEntry entry : entries) {
                    if (entry.getSequenceNumber() > sequenceNumber.get()) {
                        sequenceNumber.set(entry.getSequenceNumber());
                    }
                }

            } catch (IOException e) {
                System.err.println("Warning: Failed to load segment " + segmentPath + ": " + e.getMessage());
            }
        }

        // Use the latest segment if it's not full
        if (latestSegment != null && !latestSegment.isFull()) {
            currentSegment = latestSegment;
        }

        System.out.println("Discovered " + segments.size() + " log segments, max seq: " + sequenceNumber.get());
    }

    /**
     * Extract segment ID from filename (e.g., "segment-0000000001.log" -> 1)
     */
    private long extractSegmentId(Path path) {
        String filename = path.getFileName().toString();
        String idStr = filename.replace("segment-", "").replace(".log", "");
        return Long.parseLong(idStr);
    }

    /**
     * Create a new log segment
     */
    private void createNewSegment() throws IOException {
        lock.writeLock().lock();
        try {
            long newSegmentId = segments.isEmpty() ? 1 : Collections.max(segments.keySet()) + 1;
            String segmentName = String.format("segment-%010d.log", newSegmentId);
            Path segmentPath = logDirectory.resolve(segmentName);

            LogSegment newSegment = new LogSegment(segmentPath, newSegmentId, maxSegmentSize);
            segments.put(newSegmentId, newSegment);
            currentSegment = newSegment;

            System.out.println("Created new log segment: " + segmentName);

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Log an operation to the WAL.
     * This must be called BEFORE applying the operation to the index.
     */
    public long log(LogEntry.OperationType operationType, String indexName, byte[] data) throws IOException {
        if (closed) {
            throw new IOException("Transaction log is closed");
        }

        long seq = sequenceNumber.incrementAndGet();
        LogEntry entry = new LogEntry(seq, operationType, indexName, data);

        // Check if entry is too large for any segment
        int entrySize = entry.getSize() + 4; // +4 for length prefix
        if (entrySize + 16 > maxSegmentSize) { // +16 for header
            throw new IOException("Entry size (" + entrySize + " bytes) exceeds max segment size (" + maxSegmentSize + " bytes)");
        }

        lock.readLock().lock();
        try {
            // Try to append to current segment
            if (!currentSegment.append(entry)) {
                // Current segment is full - need new segment
                lock.readLock().unlock();
                lock.writeLock().lock();
                try {
                    // Double-check after acquiring write lock - another thread might have already rotated
                    if (!currentSegment.append(entry)) {
                        // Still can't append, create new segment
                        createNewSegment();

                        // Append to new segment
                        if (!currentSegment.append(entry)) {
                            throw new IOException("Failed to append entry to new segment");
                        }
                    }

                } finally {
                    lock.readLock().lock();
                    lock.writeLock().unlock();
                }
            }

            return seq;

        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Force sync current segment to disk
     */
    public void sync() throws IOException {
        if (currentSegment != null && !closed) {
            currentSegment.sync();
        }
    }

    /**
     * Mark a checkpoint - operations before this point have been safely persisted to the index
     */
    public void checkpoint(long sequenceNumber) throws IOException {
        lock.writeLock().lock();
        try {
            this.lastCheckpointSeq = sequenceNumber;

            // Sync current segment
            if (currentSegment != null) {
                currentSegment.sync();
            }

            // Clean up old segments (keep some for safety)
            cleanupOldSegments();

            System.out.println("Checkpoint created at sequence: " + sequenceNumber);

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Clean up segments that are older than the last checkpoint
     */
    private void cleanupOldSegments() throws IOException {
        if (segments.size() <= MAX_SEGMENTS_TO_KEEP) {
            return; // Keep minimum number of segments
        }

        List<Long> segmentIds = new ArrayList<>(segments.keySet());
        Collections.sort(segmentIds);

        // Keep segments that might contain uncommitted entries
        int toDelete = segmentIds.size() - MAX_SEGMENTS_TO_KEEP;
        if (toDelete <= 0) {
            return;
        }

        for (int i = 0; i < toDelete; i++) {
            long segmentId = segmentIds.get(i);
            LogSegment segment = segments.get(segmentId);

            if (segment != null && segment != currentSegment) {
                // Check if all entries in this segment are before checkpoint
                List<LogEntry> entries = segment.readAllEntries();
                boolean allBeforeCheckpoint = entries.stream()
                        .allMatch(e -> e.getSequenceNumber() <= lastCheckpointSeq);

                if (allBeforeCheckpoint) {
                    segment.delete();
                    segments.remove(segmentId);
                    System.out.println("Deleted old log segment: " + segmentId);
                }
            }
        }
    }

    /**
     * Read all entries from the log (for recovery)
     */
    public List<LogEntry> readAllEntries() throws IOException {
        List<LogEntry> allEntries = new ArrayList<>();

        lock.readLock().lock();
        try {
            List<Long> segmentIds = new ArrayList<>(segments.keySet());
            Collections.sort(segmentIds);

            for (long segmentId : segmentIds) {
                LogSegment segment = segments.get(segmentId);
                if (segment != null) {
                    allEntries.addAll(segment.readAllEntries());
                }
            }

        } finally {
            lock.readLock().unlock();
        }

        // Sort by sequence number to ensure correct order
        allEntries.sort(Comparator.comparingLong(LogEntry::getSequenceNumber));
        return allEntries;
    }

    /**
     * Read entries after a specific sequence number (for incremental recovery)
     */
    public List<LogEntry> readEntriesAfter(long afterSequence) throws IOException {
        return readAllEntries().stream()
                .filter(e -> e.getSequenceNumber() > afterSequence)
                .collect(Collectors.toList());
    }

    /**
     * Get statistics about the transaction log
     */
    public LogStats getStats() {
        lock.readLock().lock();
        try {
            long totalSize = 0;
            long totalEntries = 0;

            for (LogSegment segment : segments.values()) {
                totalSize += segment.getCurrentSize();
                totalEntries += segment.getEntryCount();
            }

            return new LogStats(
                    segments.size(),
                    totalSize,
                    totalEntries,
                    sequenceNumber.get(),
                    lastCheckpointSeq,
                    currentSegment != null ? currentSegment.getSegmentId() : -1
            );

        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;

            lock.writeLock().lock();
            try {
                // Close all segments
                for (LogSegment segment : segments.values()) {
                    try {
                        segment.close();
                    } catch (IOException e) {
                        System.err.println("Error closing segment: " + e.getMessage());
                    }
                }
                segments.clear();
                currentSegment = null;

            } finally {
                lock.writeLock().unlock();
            }
        }
    }

    /**
     * Statistics about the transaction log
     */
    public static class LogStats {
        public final int segmentCount;
        public final long totalSize;
        public final long totalEntries;
        public final long currentSequence;
        public final long lastCheckpoint;
        public final long currentSegmentId;

        public LogStats(int segmentCount, long totalSize, long totalEntries,
                       long currentSequence, long lastCheckpoint, long currentSegmentId) {
            this.segmentCount = segmentCount;
            this.totalSize = totalSize;
            this.totalEntries = totalEntries;
            this.currentSequence = currentSequence;
            this.lastCheckpoint = lastCheckpoint;
            this.currentSegmentId = currentSegmentId;
        }

        @Override
        public String toString() {
            return String.format(
                    "LogStats{segments=%d, size=%d bytes, entries=%d, seq=%d, checkpoint=%d, currentSeg=%d}",
                    segmentCount, totalSize, totalEntries, currentSequence, lastCheckpoint, currentSegmentId
            );
        }
    }

    public long getLastCheckpointSeq() {
        return lastCheckpointSeq;
    }

    public long getCurrentSequence() {
        return sequenceNumber.get();
    }

    public Path getLogDirectory() {
        return logDirectory;
    }
}

