package io.synapsedb.index.operations;

import io.synapsedb.exception.InvalidIndexStateException;
import io.synapsedb.index.Index;

import java.io.IOException;

/**
 * Operations related to segment merging.
 * Controls how index segments are merged to optimize search performance.
 *
 * @author Amit Tiwari
 */
public class MergeOperations {

    private final Index index;

    public MergeOperations(Index index) {
        this.index = index;
    }

    /**
     * Force merge to the specified number of segments.
     *
     * @param maxNumSegments target number of segments (1 = full optimization)
     * @throws IOException if merge fails
     */
    public void forceMerge(int maxNumSegments) throws IOException, InvalidIndexStateException {
        if (maxNumSegments < 1) {
            throw new IllegalArgumentException("maxNumSegments must be >= 1");
        }
        index.forceMerge(maxNumSegments);
    }

    /**
     * Optimize index to a single segment.
     * Most expensive but gives best search performance.
     * Use during off-peak hours.
     *
     * @throws IOException if optimization fails
     */
    public void optimize() throws IOException, InvalidIndexStateException {
        index.forceMerge(1);
    }

    /**
     * Force merge to reduce segments to a reasonable number.
     * Less aggressive than full optimization.
     *
     * @throws IOException if merge fails
     */
    public void mergeToOptimalSegments() throws IOException, InvalidIndexStateException {
        // Merge to 5 segments - good balance between search speed and merge cost
        index.forceMerge(5);
    }

    /**
     * Optimize and then commit.
     * Ensures optimized state is durable.
     *
     * @throws IOException if operation fails
     */
    public void optimizeAndCommit() throws IOException, InvalidIndexStateException {
        optimize();
        index.commit();
    }

    /**
     * Full optimization cycle: merge, commit, refresh.
     *
     * @throws IOException if operation fails
     */
    public void optimizeCommitAndRefresh() throws IOException, InvalidIndexStateException {
        optimize();
        index.commit();
        index.refresh();
    }

    /**
     * Get merge statistics.
     *
     * @return total number of merges performed
     */
    public long getMergeCount() {
        return index.getStats().getMergeTotal();
    }

    /**
     * Get total time spent merging.
     *
     * @return total merge time in milliseconds
     */
    public long getTotalMergeTimeMs() {
        return index.getStats().getMergeTotalTimeInMillis();
    }

    /**
     * Get average merge time.
     *
     * @return average merge time in milliseconds
     */
    public double getAverageMergeTimeMs() {
        long count = getMergeCount();
        if (count == 0) return 0.0;
        return (double) getTotalMergeTimeMs() / count;
    }

    /**
     * Get total bytes merged.
     *
     * @return total size of merged segments in bytes
     */
    public long getTotalMergeBytes() {
        return index.getStats().getMergeTotalSizeInBytes();
    }

    /**
     * Check if a merge is currently in progress.
     *
     * @return true if merging
     */
    public boolean isMergeInProgress() {
        return index.getStats().getMergeCurrent() > 0;
    }

    /**
     * Get current number of merges in progress.
     *
     * @return number of concurrent merges
     */
    public long getCurrentMergeCount() {
        return index.getStats().getMergeCurrent();
    }
}
