package io.synapsedb.index.operations;

import io.synapsedb.index.Index;

import java.io.IOException;

/**
 * Operations related to index refresh.
 * Controls when indexed documents become searchable.
 *
 * @author Amit Tiwari
 */
public class RefreshOperations {

    private final Index index;

    public RefreshOperations(Index index) {
        this.index = index;
    }

    /**
     * Refresh the index to make recent changes searchable.
     * This is a relatively lightweight operation.
     *
     * @throws IOException if refresh fails
     */
    public void refresh() throws IOException {
        index.refresh();
    }

    /**
     * Refresh and return the time taken.
     *
     * @return refresh time in milliseconds
     * @throws IOException if refresh fails
     */
    public long refreshWithTiming() throws IOException {
        long start = System.currentTimeMillis();
        index.refresh();
        return System.currentTimeMillis() - start;
    }

    /**
     * Get refresh statistics.
     *
     * @return total number of refreshes performed
     */
    public long getRefreshCount() {
        return index.getStats().getRefreshTotal();
    }

    /**
     * Get total time spent refreshing.
     *
     * @return total refresh time in milliseconds
     */
    public long getTotalRefreshTimeMs() {
        return index.getStats().getRefreshTotalTimeInMillis();
    }

    /**
     * Get average refresh time.
     *
     * @return average refresh time in milliseconds
     */
    public double getAverageRefreshTimeMs() {
        long count = getRefreshCount();
        if (count == 0) return 0.0;
        return (double) getTotalRefreshTimeMs() / count;
    }

    /**
     * Get time of last refresh.
     *
     * @return timestamp of last refresh in milliseconds since epoch
     */
    public long getLastRefreshTime() {
        return index.getStats().getLastRefreshTime();
    }

    /**
     * Get time since last refresh.
     *
     * @return milliseconds since last refresh
     */
    public long getTimeSinceLastRefreshMs() {
        long lastRefresh = getLastRefreshTime();
        if (lastRefresh == 0) return -1;
        return System.currentTimeMillis() - lastRefresh;
    }

    /**
     * Check if auto-refresh is enabled.
     *
     * @return true if auto-refresh is enabled
     */
    public boolean isAutoRefreshEnabled() {
        return index.getSettings().isAutoRefresh();
    }

    /**
     * Get auto-refresh interval.
     *
     * @return refresh interval in milliseconds
     */
    public long getRefreshIntervalMs() {
        return index.getSettings().getRefreshIntervalMs();
    }

    /**
     * Check if a refresh is needed based on time since last refresh.
     *
     * @return true if time since last refresh exceeds configured interval
     */
    public boolean isRefreshNeeded() {
        if (!isAutoRefreshEnabled()) {
            return false;
        }
        long timeSinceRefresh = getTimeSinceLastRefreshMs();
        return timeSinceRefresh < 0 || timeSinceRefresh >= getRefreshIntervalMs();
    }

    /**
     * Refresh statistics summary.
     *
     * @return formatted statistics string
     */
    public String getRefreshStatistics() {
        return String.format(
            "Refresh Statistics: count=%d, total_time=%dms, avg_time=%.2fms, last_refresh=%dms_ago",
            getRefreshCount(),
            getTotalRefreshTimeMs(),
            getAverageRefreshTimeMs(),
            getTimeSinceLastRefreshMs()
        );
    }
}
