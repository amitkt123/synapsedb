package io.synapsedb.durability;

import org.junit.jupiter.api.*;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the Write-Ahead Log implementation
 *
 * @author Amit Tiwari
 */
class TransactionLogTest {

    private Path testDirectory;
    private TransactionLog transactionLog;

    @BeforeEach
    void setUp() throws IOException {
        testDirectory = Files.createTempDirectory("synapse-wal-test");
        transactionLog = new TransactionLog(testDirectory);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (transactionLog != null) {
            transactionLog.close();
        }
        // Clean up test directory
        deleteDirectory(testDirectory);
    }

    @Test
    @DisplayName("Should create WAL directory on initialization")
    void testWALDirectoryCreation() {
        Path walDir = testDirectory.resolve("wal");
        assertTrue(Files.exists(walDir), "WAL directory should exist");
        assertTrue(Files.isDirectory(walDir), "WAL path should be a directory");
    }

    @Test
    @DisplayName("Should log and read single entry")
    void testLogAndReadSingleEntry() throws IOException {
        // Log an ADD_DOCUMENT operation
        byte[] data = "test document data".getBytes();
        long seq = transactionLog.log(LogEntry.OperationType.ADD_DOCUMENT, "test-index", data);

        assertTrue(seq > 0, "Sequence number should be positive");

        // Read all entries
        List<LogEntry> entries = transactionLog.readAllEntries();
        assertEquals(1, entries.size(), "Should have one entry");

        LogEntry entry = entries.get(0);
        assertEquals(seq, entry.getSequenceNumber());
        assertEquals(LogEntry.OperationType.ADD_DOCUMENT, entry.getOperationType());
        assertEquals("test-index", entry.getIndexName());
        assertArrayEquals(data, entry.getData());
        assertTrue(entry.isValid(), "Entry should be valid");
    }

    @Test
    @DisplayName("Should log multiple entries in sequence")
    void testLogMultipleEntries() throws IOException {
        int count = 100;
        for (int i = 0; i < count; i++) {
            byte[] data = ("document-" + i).getBytes();
            transactionLog.log(LogEntry.OperationType.ADD_DOCUMENT, "test-index", data);
        }

        List<LogEntry> entries = transactionLog.readAllEntries();
        assertEquals(count, entries.size(), "Should have " + count + " entries");

        // Verify sequence numbers are monotonic
        for (int i = 0; i < entries.size(); i++) {
            assertEquals(i + 1, entries.get(i).getSequenceNumber());
        }
    }

    @Test
    @DisplayName("Should handle different operation types")
    void testDifferentOperationTypes() throws IOException {
        transactionLog.log(LogEntry.OperationType.ADD_DOCUMENT, "idx1", "add".getBytes());
        transactionLog.log(LogEntry.OperationType.UPDATE_DOCUMENT, "idx1", "update".getBytes());
        transactionLog.log(LogEntry.OperationType.DELETE_DOCUMENT, "idx1", "delete".getBytes());
        transactionLog.log(LogEntry.OperationType.COMMIT, "idx1", new byte[0]);

        List<LogEntry> entries = transactionLog.readAllEntries();
        assertEquals(4, entries.size());

        assertEquals(LogEntry.OperationType.ADD_DOCUMENT, entries.get(0).getOperationType());
        assertEquals(LogEntry.OperationType.UPDATE_DOCUMENT, entries.get(1).getOperationType());
        assertEquals(LogEntry.OperationType.DELETE_DOCUMENT, entries.get(2).getOperationType());
        assertEquals(LogEntry.OperationType.COMMIT, entries.get(3).getOperationType());
    }

    @Test
    @DisplayName("Should create new segment when current is full")
    void testSegmentRotation() throws IOException {
        // Create log with very small segment size
        transactionLog.close();
        transactionLog = new TransactionLog(testDirectory, 1024); // 1KB segments

        // Log enough data to fill multiple segments
        byte[] largeData = new byte[500]; // 500 bytes per entry
        for (int i = 0; i < 10; i++) {
            transactionLog.log(LogEntry.OperationType.ADD_DOCUMENT, "test-index", largeData);
        }

        TransactionLog.LogStats stats = transactionLog.getStats();
        assertTrue(stats.segmentCount > 1, "Should have created multiple segments");
    }

    @Test
    @DisplayName("Should sync data to disk")
    void testSync() throws IOException {
        transactionLog.log(LogEntry.OperationType.ADD_DOCUMENT, "idx", "data".getBytes());

        // Sync should not throw exception
        assertDoesNotThrow(() -> transactionLog.sync());
    }

    @Test
    @DisplayName("Should create checkpoint and cleanup old segments")
    void testCheckpointAndCleanup() throws IOException {
        // Log some entries
        for (int i = 0; i < 50; i++) {
            transactionLog.log(LogEntry.OperationType.ADD_DOCUMENT, "test-index", ("doc-" + i).getBytes());
        }

        long currentSeq = transactionLog.getCurrentSequence();

        // Create checkpoint
        transactionLog.checkpoint(currentSeq);

        assertEquals(currentSeq, transactionLog.getLastCheckpointSeq());
    }

    @Test
    @DisplayName("Should read entries after specific sequence")
    void testReadEntriesAfter() throws IOException {
        // Log 10 entries
        for (int i = 0; i < 10; i++) {
            transactionLog.log(LogEntry.OperationType.ADD_DOCUMENT, "idx", ("doc-" + i).getBytes());
        }

        // Read entries after sequence 5
        List<LogEntry> entries = transactionLog.readEntriesAfter(5);
        assertEquals(5, entries.size(), "Should have 5 entries after sequence 5");

        // Verify they're the right entries
        assertEquals(6, entries.get(0).getSequenceNumber());
        assertEquals(10, entries.get(4).getSequenceNumber());
    }

    @Test
    @DisplayName("Should recover from restart")
    void testRecoveryAfterRestart() throws IOException {
        // Log some entries
        transactionLog.log(LogEntry.OperationType.ADD_DOCUMENT, "idx", "doc1".getBytes());
        transactionLog.log(LogEntry.OperationType.ADD_DOCUMENT, "idx", "doc2".getBytes());
        transactionLog.log(LogEntry.OperationType.COMMIT, "idx", new byte[0]);

        long lastSeq = transactionLog.getCurrentSequence();

        // Close and reopen
        transactionLog.close();
        transactionLog = new TransactionLog(testDirectory);

        // Should recover previous sequence
        assertEquals(lastSeq, transactionLog.getCurrentSequence());

        // Should be able to read all entries
        List<LogEntry> entries = transactionLog.readAllEntries();
        assertEquals(3, entries.size());
    }

    @Test
    @DisplayName("Should provide accurate statistics")
    void testStatistics() throws IOException {
        transactionLog.log(LogEntry.OperationType.ADD_DOCUMENT, "idx", "data1".getBytes());
        transactionLog.log(LogEntry.OperationType.ADD_DOCUMENT, "idx", "data2".getBytes());

        TransactionLog.LogStats stats = transactionLog.getStats();

        assertNotNull(stats);
        assertTrue(stats.segmentCount > 0);
        assertTrue(stats.totalSize > 0);
        assertEquals(2, stats.totalEntries);
        assertEquals(2, stats.currentSequence);
    }

    @Test
    @DisplayName("Should handle empty log")
    void testEmptyLog() throws IOException {
        List<LogEntry> entries = transactionLog.readAllEntries();
        assertNotNull(entries);
        assertTrue(entries.isEmpty());

        TransactionLog.LogStats stats = transactionLog.getStats();
        assertEquals(0, stats.totalEntries);
    }

    @Test
    @DisplayName("Should handle large data entries")
    void testLargeDataEntries() throws IOException {
        // 1MB of data
        byte[] largeData = new byte[1024 * 1024];
        for (int i = 0; i < largeData.length; i++) {
            largeData[i] = (byte) (i % 256);
        }

        long seq = transactionLog.log(LogEntry.OperationType.ADD_DOCUMENT, "idx", largeData);

        List<LogEntry> entries = transactionLog.readAllEntries();
        assertEquals(1, entries.size());
        assertArrayEquals(largeData, entries.get(0).getData());
    }

    // Helper method to delete directory recursively
    private void deleteDirectory(Path directory) throws IOException {
        if (Files.exists(directory)) {
            Files.walk(directory)
                    .sorted(java.util.Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            // Ignore
                        }
                    });
        }
    }
}

