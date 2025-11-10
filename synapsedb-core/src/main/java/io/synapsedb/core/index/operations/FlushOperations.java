package io.synapsedb.core.index.operations;

import io.synapsedb.core.exception.InvalidIndexStateException;
import io.synapsedb.core.index.Index;

import java.io.IOException;

/**
 * Operations related to flushing index changes.
 * Controls when and how buffered changes are written to disk.
 *
 * @author Amit Tiwari
 */
public class FlushOperations {

    private final Index index;

    public FlushOperations(Index index) {
        this.index = index;
    }

    /**
     * Flush all pending changes to disk.
     * Does not commit - changes may not survive a crash.
     *
     * @throws IOException if flush fails
     */
    public void flush() throws IOException, InvalidIndexStateException {
        index.flush();
    }

    /**
     * Flush and then commit.
     * Ensures durability of changes.
     *
     * @throws IOException if operation fails
     */
    public void flushAndCommit() throws IOException, InvalidIndexStateException {
        index.flush();
        index.commit();
    }

    /**
     * Flush, commit, and refresh.
     * Complete cycle for durability and searchability.
     *
     * @throws IOException if operation fails
     */
    public void flushCommitAndRefresh() throws IOException, InvalidIndexStateException {
        index.flush();
        index.commit();
        index.refresh();
    }

    /**
     * Get flush statistics.
     *
     * @return total number of flushes performed
     */
    public long getFlushCount() {
        return index.getStats().getFlushTotal();
    }

    /**
     * Get total time spent flushing.
     *
     * @return total flush time in milliseconds
     */
    public long getTotalFlushTimeMs() {
        return index.getStats().getFlushTotalTimeInMillis();
    }

    /**
     * Get average flush time.
     *
     * @return average flush time in milliseconds
     */
    public double getAverageFlushTimeMs() {
        long count = getFlushCount();
        if (count == 0) return 0.0;
        return (double) getTotalFlushTimeMs() / count;
    }

    /**
     * Get the last flush time.
     *
     * @return timestamp of last flush in milliseconds since epoch
     */
    public long getLastFlushTime() {
        return index.getStats().getLastFlushTime();
    }

    /**
     * Get time since last flush.
     *
     * @return milliseconds since last flush, or -1 if never flushed
     */
    public long getTimeSinceLastFlushMs() {
        long lastFlush = getLastFlushTime();
        if (lastFlush == 0) return -1;
        return System.currentTimeMillis() - lastFlush;
    }
}
