package io.synapsedb.recovery;

import io.synapsedb.document.Document;
import io.synapsedb.durability.LogEntry;
import io.synapsedb.durability.TransactionLog;
import io.synapsedb.index.Index;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages crash recovery by replaying Write-Ahead Log entries.
 *
 * Recovery Process:
 * 1. Read checkpoint file to find last safe point
 * 2. Read WAL entries after checkpoint
 * 3. Replay operations in sequence
 * 4. Mark recovery complete
 *
 * @author Amit Tiwari
 */
public class RecoveryManager {

    private final Path baseDirectory;
    private final TransactionLog transactionLog;
    private final Map<String, RecoveryStats> statsPerIndex;

    public RecoveryManager(Path baseDirectory) throws IOException {
        this.baseDirectory = baseDirectory;
        this.transactionLog = new TransactionLog(baseDirectory);
        this.statsPerIndex = new ConcurrentHashMap<>();
    }

    /**
     * Perform recovery for an index.
     * This should be called during index initialization if recovery is needed.
     */
    public RecoveryResult recoverIndex(String indexName, Index index) throws IOException {
        System.out.println("Starting recovery for index: " + indexName);
        long startTime = System.currentTimeMillis();

        RecoveryStats stats = new RecoveryStats(indexName);
        statsPerIndex.put(indexName, stats);

        try {
            // Read checkpoint sequence (if exists)
            long checkpointSeq = readCheckpointSequence(indexName);
            stats.checkpointSequence = checkpointSeq;

            // Read log entries after checkpoint
            List<LogEntry> entries = checkpointSeq > 0
                    ? transactionLog.readEntriesAfter(checkpointSeq)
                    : transactionLog.readAllEntries();

            // Filter entries for this index
            List<LogEntry> indexEntries = entries.stream()
                    .filter(e -> indexName.equals(e.getIndexName()))
                    .toList();

            System.out.println("Found " + indexEntries.size() + " log entries to replay for " + indexName);

            // Replay entries
            for (LogEntry entry : indexEntries) {
                try {
                    replayEntry(entry, index);
                    stats.successfulReplays++;
                } catch (Exception e) {
                    stats.failedReplays++;
                    stats.errors.add("Failed to replay seq " + entry.getSequenceNumber() + ": " + e.getMessage());
                    System.err.println("Recovery error: " + e.getMessage());
                }
            }

            // Commit all replayed operations
            if (stats.successfulReplays > 0) {
                index.commit();
                index.refresh();
            }

            stats.durationMs = System.currentTimeMillis() - startTime;
            stats.success = stats.failedReplays == 0;

            System.out.println("Recovery completed: " + stats);
            return new RecoveryResult(stats.success, stats);

        } catch (Exception e) {
            stats.success = false;
            stats.errors.add("Recovery failed: " + e.getMessage());
            throw new IOException("Recovery failed for index " + indexName, e);
        }
    }

    /**
     * Replay a single log entry
     */
    private void replayEntry(LogEntry entry, Index index) throws Exception {
        byte[] data = entry.getData();

        switch (entry.getOperationType()) {
            case ADD_DOCUMENT:
                Document addDoc = deserializeDocument(data);
                index.addDocument(addDoc);
                break;

            case UPDATE_DOCUMENT:
                Document updateDoc = deserializeDocument(data);
                index.updateDocument(updateDoc.getId(), updateDoc);
                break;

            case DELETE_DOCUMENT:
                String docId = deserializeDocumentId(data);
                index.deleteDocument(docId);
                break;

            case COMMIT:
                index.commit();
                break;

            case FLUSH:
                index.flush();
                break;

            case CHECKPOINT:
                // Checkpoint entries are informational only
                break;

            default:
                throw new IllegalStateException("Unknown operation type: " + entry.getOperationType());
        }
    }

    /**
     * Deserialize a document from bytes
     */
    private Document deserializeDocument(byte[] data) throws IOException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            return (Document) ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize document", e);
        }
    }

    /**
     * Deserialize a document ID from bytes
     */
    private String deserializeDocumentId(byte[] data) {
        return new String(data);
    }

    /**
     * Read the checkpoint sequence for an index
     */
    private long readCheckpointSequence(String indexName) {
        try {
            Path checkpointFile = baseDirectory.resolve("checkpoints").resolve(indexName + ".checkpoint");
            if (!checkpointFile.toFile().exists()) {
                return 0;
            }

            try (DataInputStream dis = new DataInputStream(new FileInputStream(checkpointFile.toFile()))) {
                return dis.readLong();
            }

        } catch (IOException e) {
            System.err.println("Failed to read checkpoint: " + e.getMessage());
            return 0;
        }
    }

    /**
     * Write checkpoint sequence for an index
     */
    public void writeCheckpoint(String indexName, long sequence) throws IOException {
        Path checkpointDir = baseDirectory.resolve("checkpoints");
        checkpointDir.toFile().mkdirs();

        Path checkpointFile = checkpointDir.resolve(indexName + ".checkpoint");

        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(checkpointFile.toFile()))) {
            dos.writeLong(sequence);
            dos.flush();
        }

        // Also tell transaction log about checkpoint
        transactionLog.checkpoint(sequence);
    }

    /**
     * Check if recovery is needed for an index
     */
    public boolean isRecoveryNeeded(String indexName) throws IOException {
        long checkpointSeq = readCheckpointSequence(indexName);
        long currentSeq = transactionLog.getCurrentSequence();

        // If current sequence is greater than checkpoint, recovery may be needed
        if (currentSeq > checkpointSeq) {
            // Check if there are entries for this index
            List<LogEntry> entries = transactionLog.readEntriesAfter(checkpointSeq);
            return entries.stream().anyMatch(e -> indexName.equals(e.getIndexName()));
        }

        return false;
    }

    /**
     * Get recovery statistics for an index
     */
    public RecoveryStats getStats(String indexName) {
        return statsPerIndex.get(indexName);
    }

    /**
     * Get the transaction log
     */
    public TransactionLog getTransactionLog() {
        return transactionLog;
    }

    public void close() throws IOException {
        transactionLog.close();
    }

    /**
     * Statistics about a recovery operation
     */
    public static class RecoveryStats {
        public final String indexName;
        public long checkpointSequence;
        public int successfulReplays;
        public int failedReplays;
        public long durationMs;
        public boolean success;
        public final java.util.List<String> errors = new java.util.ArrayList<>();

        public RecoveryStats(String indexName) {
            this.indexName = indexName;
        }

        @Override
        public String toString() {
            return String.format(
                    "RecoveryStats{index='%s', checkpoint=%d, replayed=%d, failed=%d, duration=%dms, success=%s}",
                    indexName, checkpointSequence, successfulReplays, failedReplays, durationMs, success
            );
        }
    }

    /**
     * Result of a recovery operation
     */
    public static class RecoveryResult {
        public final boolean success;
        public final RecoveryStats stats;

        public RecoveryResult(boolean success, RecoveryStats stats) {
            this.success = success;
            this.stats = stats;
        }

        public boolean isSuccess() {
            return success;
        }

        public RecoveryStats getStats() {
            return stats;
        }
    }
}

