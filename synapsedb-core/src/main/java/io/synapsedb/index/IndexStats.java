package io.synapsedb.index;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Real-time statistics for index operations.
 * Thread-safe implementation using atomic operations.
 */
public class IndexStats {

    private final String indexName;
    private final long createdAt;

    // Document statistics
    private final AtomicLong totalDocs = new AtomicLong(0);
    private final AtomicLong deletedDocs = new AtomicLong(0);
    private final AtomicLong maxDoc = new AtomicLong(0);

    // Indexing statistics
    private final LongAdder indexTotal = new LongAdder();
    private final LongAdder indexTimeInMillis = new LongAdder();
    private final LongAdder indexCurrent = new LongAdder();
    private final LongAdder indexFailed = new LongAdder();
    private final AtomicLong lastIndexTime = new AtomicLong(0);

    // Search statistics
    private final LongAdder searchQueryTotal = new LongAdder();
    private final LongAdder searchQueryTimeInMillis = new LongAdder();
    private final LongAdder searchQueryCurrent = new LongAdder();
    private final LongAdder searchFetchTotal = new LongAdder();
    private final LongAdder searchFetchTimeInMillis = new LongAdder();
    private final AtomicLong lastSearchTime = new AtomicLong(0);

    // Get statistics
    private final LongAdder getTotal = new LongAdder();
    private final LongAdder getTimeInMillis = new LongAdder();
    private final LongAdder getExistsTotal = new LongAdder();
    private final LongAdder getExistsTimeInMillis = new LongAdder();
    private final LongAdder getMissingTotal = new LongAdder();
    private final LongAdder getMissingTimeInMillis = new LongAdder();

    // Refresh statistics
    private final LongAdder refreshTotal = new LongAdder();
    private final LongAdder refreshTotalTimeInMillis = new LongAdder();
    private final AtomicLong lastRefreshTime = new AtomicLong(0);

    // Flush statistics
    private final LongAdder flushTotal = new LongAdder();
    private final LongAdder flushTotalTimeInMillis = new LongAdder();
    private final AtomicLong lastFlushTime = new AtomicLong(0);

    // Merge statistics
    private final LongAdder mergeTotal = new LongAdder();
    private final LongAdder mergeTotalTimeInMillis = new LongAdder();
    private final LongAdder mergeTotalSizeInBytes = new LongAdder();
    private final LongAdder mergeCurrent = new LongAdder();

    // Storage statistics
    private final AtomicLong storeSizeInBytes = new AtomicLong(0);
    private final AtomicLong translogSizeInBytes = new AtomicLong(0);

    // Memory statistics
    private final AtomicLong memorySizeInBytes = new AtomicLong(0);
    private final AtomicLong termsSizeInBytes = new AtomicLong(0);
    private final AtomicLong storedFieldsSizeInBytes = new AtomicLong(0);
    private final AtomicLong termVectorsSizeInBytes = new AtomicLong(0);
    private final AtomicLong normsSizeInBytes = new AtomicLong(0);
    private final AtomicLong pointsSizeInBytes = new AtomicLong(0);
    private final AtomicLong docValuesSizeInBytes = new AtomicLong(0);

    public IndexStats(String indexName) {
        this.indexName = indexName;
        this.createdAt = Instant.now().toEpochMilli();
    }

    // Document operations
    public void updateDocumentStats(long total, long deleted, long max) {
        totalDocs.set(total);
        deletedDocs.set(deleted);
        maxDoc.set(max);
    }

    // Indexing operations
    public void recordIndexing(long timeInMillis, boolean success) {
        indexTotal.increment();
        indexTimeInMillis.add(timeInMillis);
        if (!success) {
            indexFailed.increment();
        }
        lastIndexTime.set(Instant.now().toEpochMilli());
    }

    public void incrementIndexingCurrent() {
        indexCurrent.increment();
    }

    public void decrementIndexingCurrent() {
        indexCurrent.decrement();
    }

    // Search operations
    public void recordSearch(long queryTimeInMillis, long fetchTimeInMillis) {
        searchQueryTotal.increment();
        searchQueryTimeInMillis.add(queryTimeInMillis);
        searchFetchTotal.increment();
        searchFetchTimeInMillis.add(fetchTimeInMillis);
        lastSearchTime.set(Instant.now().toEpochMilli());
    }

    public void incrementSearchCurrent() {
        searchQueryCurrent.increment();
    }

    public void decrementSearchCurrent() {
        searchQueryCurrent.decrement();
    }

    // Get operations
    public void recordGet(long timeInMillis, boolean exists) {
        getTotal.increment();
        getTimeInMillis.add(timeInMillis);
        if (exists) {
            getExistsTotal.increment();
            getExistsTimeInMillis.add(timeInMillis);
        } else {
            getMissingTotal.increment();
            getMissingTimeInMillis.add(timeInMillis);
        }
    }

    // Refresh operations
    public void recordRefresh(long timeInMillis) {
        refreshTotal.increment();
        refreshTotalTimeInMillis.add(timeInMillis);
        lastRefreshTime.set(Instant.now().toEpochMilli());
    }

    // Flush operations
    public void recordFlush(long timeInMillis) {
        flushTotal.increment();
        flushTotalTimeInMillis.add(timeInMillis);
        lastFlushTime.set(Instant.now().toEpochMilli());
    }

    // Merge operations
    public void recordMerge(long timeInMillis, long sizeInBytes) {
        mergeTotal.increment();
        mergeTotalTimeInMillis.add(timeInMillis);
        mergeTotalSizeInBytes.add(sizeInBytes);
    }

    public void incrementMergeCurrent() {
        mergeCurrent.increment();
    }

    public void decrementMergeCurrent() {
        mergeCurrent.decrement();
    }

    // Storage updates
    public void updateStorageStats(long storeSize, long translogSize) {
        storeSizeInBytes.set(storeSize);
        translogSizeInBytes.set(translogSize);
    }

    // Memory updates
    public void updateMemoryStats(long totalMemory) {
        memorySizeInBytes.set(totalMemory);
    }

    public void updateSegmentMemoryStats(long terms, long storedFields, long termVectors,
                                         long norms, long points, long docValues) {
        termsSizeInBytes.set(terms);
        storedFieldsSizeInBytes.set(storedFields);
        termVectorsSizeInBytes.set(termVectors);
        normsSizeInBytes.set(norms);
        pointsSizeInBytes.set(points);
        docValuesSizeInBytes.set(docValues);
    }

    // Getters for statistics
    public long getTotalDocs() { return totalDocs.get(); }
    public long getDeletedDocs() { return deletedDocs.get(); }
    public long getMaxDoc() { return maxDoc.get(); }

    public long getIndexTotal() { return indexTotal.sum(); }
    public long getIndexTimeInMillis() { return indexTimeInMillis.sum(); }
    public long getIndexCurrent() { return indexCurrent.sum(); }
    public long getIndexFailed() { return indexFailed.sum(); }
    public long getLastIndexTime() { return lastIndexTime.get(); }

    public long getSearchQueryTotal() { return searchQueryTotal.sum(); }
    public long getSearchQueryTimeInMillis() { return searchQueryTimeInMillis.sum(); }
    public long getSearchQueryCurrent() { return searchQueryCurrent.sum(); }
    public long getSearchFetchTotal() { return searchFetchTotal.sum(); }
    public long getSearchFetchTimeInMillis() { return searchFetchTimeInMillis.sum(); }
    public long getLastSearchTime() { return lastSearchTime.get(); }

    public long getGetTotal() { return getTotal.sum(); }
    public long getGetTimeInMillis() { return getTimeInMillis.sum(); }
    public long getGetExistsTotal() { return getExistsTotal.sum(); }
    public long getGetMissingTotal() { return getMissingTotal.sum(); }

    public long getRefreshTotal() { return refreshTotal.sum(); }
    public long getRefreshTotalTimeInMillis() { return refreshTotalTimeInMillis.sum(); }
    public long getLastRefreshTime() { return lastRefreshTime.get(); }

    public long getFlushTotal() { return flushTotal.sum(); }
    public long getFlushTotalTimeInMillis() { return flushTotalTimeInMillis.sum(); }
    public long getLastFlushTime() { return lastFlushTime.get(); }

    public long getMergeTotal() { return mergeTotal.sum(); }
    public long getMergeTotalTimeInMillis() { return mergeTotalTimeInMillis.sum(); }
    public long getMergeTotalSizeInBytes() { return mergeTotalSizeInBytes.sum(); }
    public long getMergeCurrent() { return mergeCurrent.sum(); }

    public long getStoreSizeInBytes() { return storeSizeInBytes.get(); }
    public long getTranslogSizeInBytes() { return translogSizeInBytes.get(); }
    public long getMemorySizeInBytes() { return memorySizeInBytes.get(); }

    /**
     * Calculate average indexing time
     */
    public double getAverageIndexTimeInMillis() {
        long total = indexTotal.sum();
        return total > 0 ? (double) indexTimeInMillis.sum() / total : 0;
    }

    /**
     * Calculate average search time
     */
    public double getAverageSearchTimeInMillis() {
        long total = searchQueryTotal.sum();
        return total > 0 ? (double) searchQueryTimeInMillis.sum() / total : 0;
    }

    /**
     * Calculate indexing rate (docs per second)
     */
    public double getIndexingRate() {
        long timeElapsed = Instant.now().toEpochMilli() - createdAt;
        if (timeElapsed > 0) {
            return (double) indexTotal.sum() * 1000 / timeElapsed;
        }
        return 0;
    }

    /**
     * Calculate search rate (queries per second)
     */
    public double getSearchRate() {
        long timeElapsed = Instant.now().toEpochMilli() - createdAt;
        if (timeElapsed > 0) {
            return (double) searchQueryTotal.sum() * 1000 / timeElapsed;
        }
        return 0;
    }

    /**
     * Get a summary of the statistics
     */
    public String getSummary() {
        return String.format(
                "IndexStats{index='%s', docs=%d, indexTotal=%d, indexRate=%.2f/s, " +
                        "searchTotal=%d, searchRate=%.2f/s, size=%d bytes}",
                indexName, totalDocs.get(), indexTotal.sum(), getIndexingRate(),
                searchQueryTotal.sum(), getSearchRate(), storeSizeInBytes.get()
        );
    }

    @Override
    public String toString() {
        return getSummary();
    }
}