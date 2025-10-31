package io.synapsedb.index;

import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.TieredMergePolicy;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Configuration settings for a SynapseDB index.
 * Provides control over Lucene IndexWriter configuration, merge policies, and performance settings.
 * Settings that control index behavior (refresh interval, merge policy, etc.)
 * @author Amit Tiwari
 */
public class IndexSettings {

    // Defaults
    public static final int DEFAULT_NUMBER_OF_SHARDS = 1;  // For future distributed support
    public static final int DEFAULT_NUMBER_OF_REPLICAS = 0; // For future replication support
    public static final double DEFAULT_RAM_BUFFER_SIZE_MB = 16.0;
    public static final int DEFAULT_MAX_BUFFERED_DOCS = -1; // Disabled by default
    public static final long DEFAULT_REFRESH_INTERVAL_MS = 1000; // 1 second
    public static final boolean DEFAULT_AUTO_COMMIT = true;
    public static final long DEFAULT_COMMIT_INTERVAL_MS = 30000; // 30 seconds

    // Shard settings (for future use)
    private int numberOfShards = DEFAULT_NUMBER_OF_SHARDS;
    private int numberOfReplicas = DEFAULT_NUMBER_OF_REPLICAS;

    // IndexWriter settings
    private double ramBufferSizeMB = DEFAULT_RAM_BUFFER_SIZE_MB;
    private int maxBufferedDocs = DEFAULT_MAX_BUFFERED_DOCS;
    private boolean useCompoundFile = true;

    // Merge settings
    private MergePolicyType mergePolicyType = MergePolicyType.TIERED;
    private int mergeFactor = 10;
    private double maxMergedSegmentMB = 5120; // 5GB
    private double segmentsPerTier = 10.0;
    private double maxMergeAtOnce = 10.0;

    // Refresh settings
    private long refreshIntervalMs = DEFAULT_REFRESH_INTERVAL_MS;
    private boolean autoRefresh = true;

    // Commit settings
    private boolean autoCommit = DEFAULT_AUTO_COMMIT;
    private long commitIntervalMs = DEFAULT_COMMIT_INTERVAL_MS;

    // Performance settings
    private int maxThreadStates = 8;
    private boolean enableCaching = true;
    private long cacheSize =  (100 * 1024 * 1024); // 100MB

    // Custom settings
    private Map<String, Object> customSettings = new HashMap<>();

    public enum MergePolicyType {
        TIERED,
        LOG_BYTE_SIZE,
        LOG_DOC,
        NO_MERGE
    }

    // Builder pattern for easy configuration
    public static class Builder {
        private final IndexSettings settings = new IndexSettings();

        public Builder numberOfShards(int shards) {
            settings.numberOfShards = shards;
            return this;
        }

        public Builder numberOfReplicas(int replicas) {
            settings.numberOfReplicas = replicas;
            return this;
        }

        public Builder ramBufferSizeMB(double size) {
            settings.ramBufferSizeMB = size;
            return this;
        }

        public Builder maxBufferedDocs(int docs) {
            settings.maxBufferedDocs = docs;
            return this;
        }

        public Builder useCompoundFile(boolean use) {
            settings.useCompoundFile = use;
            return this;
        }

        public Builder mergePolicyType(MergePolicyType type) {
            settings.mergePolicyType = type;
            return this;
        }

        public Builder mergeFactor(int factor) {
            settings.mergeFactor = factor;
            return this;
        }

        public Builder maxMergedSegmentMB(double size) {
            settings.maxMergedSegmentMB = size;
            return this;
        }

        public Builder refreshInterval(long interval, TimeUnit unit) {
            settings.refreshIntervalMs = unit.toMillis(interval);
            return this;
        }

        public Builder autoRefresh(boolean auto) {
            settings.autoRefresh = auto;
            return this;
        }

        public Builder autoCommit(boolean auto) {
            settings.autoCommit = auto;
            return this;
        }

        public Builder commitInterval(long interval, TimeUnit unit) {
            settings.commitIntervalMs = unit.toMillis(interval);
            return this;
        }

        public Builder maxThreadStates(int states) {
            settings.maxThreadStates = states;
            return this;
        }

        public Builder enableCaching(boolean enable) {
            settings.enableCaching = enable;
            return this;
        }

        public Builder customSetting(String key, Object value) {
            settings.customSettings.put(key, value);
            return this;
        }

        public IndexSettings build() {
            return settings;
        }
    }

    // Factory methods for common configurations
    public static IndexSettings defaultSettings() {
        return new IndexSettings();
    }

    public static IndexSettings highPerformance() {
        return new Builder()
                .ramBufferSizeMB(256)
                .refreshInterval(5, TimeUnit.SECONDS)
                .maxThreadStates(16)
                .mergePolicyType(MergePolicyType.TIERED)
                .maxMergedSegmentMB(10240) // 10GB
                .build();
    }

    public static IndexSettings lowLatency() {
        return new Builder()
                .ramBufferSizeMB(32)
                .refreshInterval(100, TimeUnit.MILLISECONDS)
                .autoRefresh(true)
                .autoCommit(true)
                .commitInterval(5, TimeUnit.SECONDS)
                .build();
    }

    public static IndexSettings bulkIndexing() {
        return new Builder()
                .ramBufferSizeMB(512)
                .refreshInterval(30, TimeUnit.SECONDS)
                .autoRefresh(false)
                .autoCommit(false)
                .useCompoundFile(false)
                .build();
    }

    /**
     * Create Lucene MergePolicy from settings
     */
    public MergePolicy createMergePolicy() {
        switch (mergePolicyType) {
            case TIERED:
                TieredMergePolicy tieredPolicy = new TieredMergePolicy();
                tieredPolicy.setMaxMergedSegmentMB(maxMergedSegmentMB);
                tieredPolicy.setSegmentsPerTier(segmentsPerTier);
                tieredPolicy.setMaxMergeAtOnce((int) maxMergeAtOnce);
                return tieredPolicy;

            case LOG_BYTE_SIZE:
                LogByteSizeMergePolicy logPolicy = new LogByteSizeMergePolicy();
                logPolicy.setMergeFactor(mergeFactor);
                logPolicy.setMaxMergeMB(maxMergedSegmentMB);
                return logPolicy;
// TODO: Enable if LogDocMergePolicy is needed in future
//            case NO_MERGE:
//                return MergePolicy.NO_COMPOUND_FILES;

            default:
                return new TieredMergePolicy();
        }
    }

    // Getters
    public int getNumberOfShards() { return numberOfShards; }
    public int getNumberOfReplicas() { return numberOfReplicas; }
    public double getRamBufferSizeMB() { return ramBufferSizeMB; }
    public int getMaxBufferedDocs() { return maxBufferedDocs; }
    public boolean isUseCompoundFile() { return useCompoundFile; }
    public MergePolicyType getMergePolicyType() { return mergePolicyType; }
    public int getMergeFactor() { return mergeFactor; }
    public double getMaxMergedSegmentMB() { return maxMergedSegmentMB; }
    public long getRefreshIntervalMs() { return refreshIntervalMs; }
    public boolean isAutoRefresh() { return autoRefresh; }
    public boolean isAutoCommit() { return autoCommit; }
    public long getCommitIntervalMs() { return commitIntervalMs; }
    public int getMaxThreadStates() { return maxThreadStates; }
    public boolean isEnableCaching() { return enableCaching; }
    public long getCacheSize() { return cacheSize; }
    public Map<String, Object> getCustomSettings() { return new HashMap<>(customSettings); }

    // Clone settings
    public IndexSettings clone() {
        IndexSettings cloned = new IndexSettings();
        cloned.numberOfShards = this.numberOfShards;
        cloned.numberOfReplicas = this.numberOfReplicas;
        cloned.ramBufferSizeMB = this.ramBufferSizeMB;
        cloned.maxBufferedDocs = this.maxBufferedDocs;
        cloned.useCompoundFile = this.useCompoundFile;
        cloned.mergePolicyType = this.mergePolicyType;
        cloned.mergeFactor = this.mergeFactor;
        cloned.maxMergedSegmentMB = this.maxMergedSegmentMB;
        cloned.segmentsPerTier = this.segmentsPerTier;
        cloned.maxMergeAtOnce = this.maxMergeAtOnce;
        cloned.refreshIntervalMs = this.refreshIntervalMs;
        cloned.autoRefresh = this.autoRefresh;
        cloned.autoCommit = this.autoCommit;
        cloned.commitIntervalMs = this.commitIntervalMs;
        cloned.maxThreadStates = this.maxThreadStates;
        cloned.enableCaching = this.enableCaching;
        cloned.cacheSize = this.cacheSize;
        cloned.customSettings = new HashMap<>(this.customSettings);
        return cloned;
    }

    @Override
    public String toString() {
        return "IndexSettings{" +
                "shards=" + numberOfShards +
                ", replicas=" + numberOfReplicas +
                ", ramBufferSizeMB=" + ramBufferSizeMB +
                ", refreshIntervalMs=" + refreshIntervalMs +
                ", mergePolicyType=" + mergePolicyType +
                ", autoRefresh=" + autoRefresh +
                ", autoCommit=" + autoCommit +
                '}';
    }
}