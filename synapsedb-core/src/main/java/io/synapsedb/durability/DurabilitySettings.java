package io.synapsedb.durability;

/**
 * Configuration settings for data durability.
 * Controls when and how data is synced to disk.
 *
 * @author Amit Tiwari
 */
public class DurabilitySettings {

    private final SyncMode syncMode;
    private final long syncIntervalMs;
    private final boolean enableWAL;
    private final long checkpointIntervalMs;
    private final int checkpointOperationCount;

    /**
     * Sync modes control the trade-off between durability and performance
     */
    public enum SyncMode {
        /**
         * Sync after every operation (safest, slowest)
         * Guarantees no data loss but impacts write performance
         */
        IMMEDIATE,

        /**
         * Sync periodically (balanced)
         * May lose recent operations on crash but good performance
         */
        PERIODIC,

        /**
         * Sync on commit only (fast, some risk)
         * Only syncs when explicitly committed
         */
        ON_COMMIT,

        /**
         * Async sync (fastest, highest risk)
         * OS handles sync timing - risk of data loss on crash
         */
        ASYNC
    }

    private DurabilitySettings(Builder builder) {
        this.syncMode = builder.syncMode;
        this.syncIntervalMs = builder.syncIntervalMs;
        this.enableWAL = builder.enableWAL;
        this.checkpointIntervalMs = builder.checkpointIntervalMs;
        this.checkpointOperationCount = builder.checkpointOperationCount;
    }

    /**
     * Default settings: balanced durability and performance
     */
    public static DurabilitySettings defaultSettings() {
        return new Builder()
                .syncMode(SyncMode.PERIODIC)
                .syncIntervalMs(1000) // Sync every 1 second
                .enableWAL(true)
                .checkpointIntervalMs(60000) // Checkpoint every minute
                .checkpointOperationCount(10000) // Or after 10K operations
                .build();
    }

    /**
     * Maximum durability: sync every operation (slow but safest)
     */
    public static DurabilitySettings maximumDurability() {
        return new Builder()
                .syncMode(SyncMode.IMMEDIATE)
                .enableWAL(true)
                .checkpointIntervalMs(30000)
                .checkpointOperationCount(5000)
                .build();
    }

    /**
     * Maximum performance: async sync (fast but risky)
     */
    public static DurabilitySettings maximumPerformance() {
        return new Builder()
                .syncMode(SyncMode.ASYNC)
                .syncIntervalMs(5000)
                .enableWAL(true)
                .checkpointIntervalMs(300000) // 5 minutes
                .checkpointOperationCount(50000)
                .build();
    }

    /**
     * Development mode: minimal durability for testing
     */
    public static DurabilitySettings developmentMode() {
        return new Builder()
                .syncMode(SyncMode.ON_COMMIT)
                .enableWAL(false) // Disable WAL for faster tests
                .build();
    }

    // Getters

    public SyncMode getSyncMode() {
        return syncMode;
    }

    public long getSyncIntervalMs() {
        return syncIntervalMs;
    }

    public boolean isWALEnabled() {
        return enableWAL;
    }

    public long getCheckpointIntervalMs() {
        return checkpointIntervalMs;
    }

    public int getCheckpointOperationCount() {
        return checkpointOperationCount;
    }

    /**
     * Should sync immediately after operation?
     */
    public boolean shouldSyncImmediately() {
        return syncMode == SyncMode.IMMEDIATE;
    }

    /**
     * Should sync periodically?
     */
    public boolean shouldSyncPeriodically() {
        return syncMode == SyncMode.PERIODIC;
    }

    /**
     * Should sync on commit only?
     */
    public boolean shouldSyncOnCommit() {
        return syncMode == SyncMode.ON_COMMIT;
    }

    @Override
    public String toString() {
        return String.format(
                "DurabilitySettings{mode=%s, syncInterval=%dms, WAL=%s, checkpointInterval=%dms, checkpointOps=%d}",
                syncMode, syncIntervalMs, enableWAL, checkpointIntervalMs, checkpointOperationCount
        );
    }

    /**
     * Builder for DurabilitySettings
     */
    public static class Builder {
        private SyncMode syncMode = SyncMode.PERIODIC;
        private long syncIntervalMs = 1000;
        private boolean enableWAL = true;
        private long checkpointIntervalMs = 60000;
        private int checkpointOperationCount = 10000;

        public Builder syncMode(SyncMode syncMode) {
            this.syncMode = syncMode;
            return this;
        }

        public Builder syncIntervalMs(long syncIntervalMs) {
            if (syncIntervalMs < 0) {
                throw new IllegalArgumentException("Sync interval must be >= 0");
            }
            this.syncIntervalMs = syncIntervalMs;
            return this;
        }

        public Builder enableWAL(boolean enableWAL) {
            this.enableWAL = enableWAL;
            return this;
        }

        public Builder checkpointIntervalMs(long checkpointIntervalMs) {
            if (checkpointIntervalMs < 0) {
                throw new IllegalArgumentException("Checkpoint interval must be >= 0");
            }
            this.checkpointIntervalMs = checkpointIntervalMs;
            return this;
        }

        public Builder checkpointOperationCount(int checkpointOperationCount) {
            if (checkpointOperationCount < 0) {
                throw new IllegalArgumentException("Checkpoint operation count must be >= 0");
            }
            this.checkpointOperationCount = checkpointOperationCount;
            return this;
        }

        public DurabilitySettings build() {
            return new DurabilitySettings(this);
        }
    }
}

