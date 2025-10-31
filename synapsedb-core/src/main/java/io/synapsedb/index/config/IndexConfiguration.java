package io.synapsedb.index.config;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Global configuration for the index system.
 * Provides system-wide defaults and configuration loading.
 * @author Amit Tiwari
 */
public class IndexConfiguration {

    // Default paths
    private static final String DEFAULT_BASE_PATH = "data/indices";
    private static final String DEFAULT_TEMP_PATH = "data/temp";
    private static final String DEFAULT_BACKUP_PATH = "data/backups";

    // System limits
    private static final int DEFAULT_MAX_INDICES = 1000;
    private static final int DEFAULT_MAX_SHARDS_PER_NODE = 1000;
    private static final long DEFAULT_MAX_INDEX_SIZE_BYTES = 50L * 1024 * 1024 * 1024; // 50GB
    private static final int DEFAULT_MAX_RESULT_WINDOW = 10000;

    // Thread pool settings
    private static final int DEFAULT_INDEX_THREAD_POOL_SIZE = Runtime.getRuntime().availableProcessors();
    private static final int DEFAULT_SEARCH_THREAD_POOL_SIZE = Runtime.getRuntime().availableProcessors() * 2;
    private static final int DEFAULT_REFRESH_THREAD_POOL_SIZE = Math.min(10, Runtime.getRuntime().availableProcessors() / 2);

    // Performance defaults
    private static final boolean DEFAULT_ENABLE_QUERY_CACHE = true;
    private static final long DEFAULT_QUERY_CACHE_SIZE_BYTES = 100 * 1024 * 1024L; // 100MB
    private static final boolean DEFAULT_ENABLE_REQUEST_CACHE = true;
    private static final long DEFAULT_REQUEST_CACHE_SIZE_BYTES = 50 * 1024 * 1024L; // 50MB

    private final Properties properties;

    // Paths
    private Path basePath;
    private Path tempPath;
    private Path backupPath;

    // Limits
    private int maxIndices;
    private int maxShardsPerNode;
    private long maxIndexSizeBytes;
    private int maxResultWindow;

    // Thread pools
    private int indexThreadPoolSize;
    private int searchThreadPoolSize;
    private int refreshThreadPoolSize;

    // Caching
    private boolean enableQueryCache;
    private long queryCacheSizeBytes;
    private boolean enableRequestCache;
    private long requestCacheSizeBytes;

    // Monitoring
    private boolean enableMetrics;
    private boolean enableSlowLog;
    private long slowLogThresholdMs;

    /**
     * Create configuration with defaults
     */
    public IndexConfiguration() {
        this(new Properties());
    }

    /**
     * Create configuration from properties
     */
    public IndexConfiguration(Properties properties) {
        this.properties = properties;
        loadConfiguration();
    }

    /**
     * Load configuration from properties
     */
    private void loadConfiguration() {
        // Paths
        this.basePath = Paths.get(getProperty("index.base.path", DEFAULT_BASE_PATH));
        this.tempPath = Paths.get(getProperty("index.temp.path", DEFAULT_TEMP_PATH));
        this.backupPath = Paths.get(getProperty("index.backup.path", DEFAULT_BACKUP_PATH));

        // Limits
        this.maxIndices = getIntProperty("index.max.indices", DEFAULT_MAX_INDICES);
        this.maxShardsPerNode = getIntProperty("index.max.shards.per.node", DEFAULT_MAX_SHARDS_PER_NODE);
        this.maxIndexSizeBytes = getLongProperty("index.max.size.bytes", DEFAULT_MAX_INDEX_SIZE_BYTES);
        this.maxResultWindow = getIntProperty("index.max.result.window", DEFAULT_MAX_RESULT_WINDOW);

        // Thread pools
        this.indexThreadPoolSize = getIntProperty("thread.pool.index.size", DEFAULT_INDEX_THREAD_POOL_SIZE);
        this.searchThreadPoolSize = getIntProperty("thread.pool.search.size", DEFAULT_SEARCH_THREAD_POOL_SIZE);
        this.refreshThreadPoolSize = getIntProperty("thread.pool.refresh.size", DEFAULT_REFRESH_THREAD_POOL_SIZE);

        // Caching
        this.enableQueryCache = getBooleanProperty("cache.query.enabled", DEFAULT_ENABLE_QUERY_CACHE);
        this.queryCacheSizeBytes = getLongProperty("cache.query.size.bytes", DEFAULT_QUERY_CACHE_SIZE_BYTES);
        this.enableRequestCache = getBooleanProperty("cache.request.enabled", DEFAULT_ENABLE_REQUEST_CACHE);
        this.requestCacheSizeBytes = getLongProperty("cache.request.size.bytes", DEFAULT_REQUEST_CACHE_SIZE_BYTES);

        // Monitoring
        this.enableMetrics = getBooleanProperty("monitoring.metrics.enabled", true);
        this.enableSlowLog = getBooleanProperty("monitoring.slowlog.enabled", true);
        this.slowLogThresholdMs = getLongProperty("monitoring.slowlog.threshold.ms", 5000L);
    }

    /**
     * Get string property
     */
    private String getProperty(String key, String defaultValue) {
        return properties.getProperty(key, System.getProperty(key, defaultValue));
    }

    /**
     * Get integer property
     */
    private int getIntProperty(String key, int defaultValue) {
        String value = getProperty(key, String.valueOf(defaultValue));
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * Get long property
     */
    private long getLongProperty(String key, long defaultValue) {
        String value = getProperty(key, String.valueOf(defaultValue));
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * Get boolean property
     */
    private boolean getBooleanProperty(String key, boolean defaultValue) {
        String value = getProperty(key, String.valueOf(defaultValue));
        return Boolean.parseBoolean(value);
    }

    // ==================== Getters ====================

    public Path getBasePath() { return basePath; }
    public Path getTempPath() { return tempPath; }
    public Path getBackupPath() { return backupPath; }

    public int getMaxIndices() { return maxIndices; }
    public int getMaxShardsPerNode() { return maxShardsPerNode; }
    public long getMaxIndexSizeBytes() { return maxIndexSizeBytes; }
    public int getMaxResultWindow() { return maxResultWindow; }

    public int getIndexThreadPoolSize() { return indexThreadPoolSize; }
    public int getSearchThreadPoolSize() { return searchThreadPoolSize; }
    public int getRefreshThreadPoolSize() { return refreshThreadPoolSize; }

    public boolean isEnableQueryCache() { return enableQueryCache; }
    public long getQueryCacheSizeBytes() { return queryCacheSizeBytes; }
    public boolean isEnableRequestCache() { return enableRequestCache; }
    public long getRequestCacheSizeBytes() { return requestCacheSizeBytes; }

    public boolean isEnableMetrics() { return enableMetrics; }
    public boolean isEnableSlowLog() { return enableSlowLog; }
    public long getSlowLogThresholdMs() { return slowLogThresholdMs; }

    // ==================== Builder ====================

    public static class Builder {
        private final Properties properties = new Properties();

        public Builder basePath(String path) {
            properties.setProperty("index.base.path", path);
            return this;
        }

        public Builder tempPath(String path) {
            properties.setProperty("index.temp.path", path);
            return this;
        }

        public Builder backupPath(String path) {
            properties.setProperty("index.backup.path", path);
            return this;
        }

        public Builder maxIndices(int max) {
            properties.setProperty("index.max.indices", String.valueOf(max));
            return this;
        }

        public Builder maxShardsPerNode(int max) {
            properties.setProperty("index.max.shards.per.node", String.valueOf(max));
            return this;
        }

        public Builder maxIndexSizeBytes(long max) {
            properties.setProperty("index.max.size.bytes", String.valueOf(max));
            return this;
        }

        public Builder maxResultWindow(int max) {
            properties.setProperty("index.max.result.window", String.valueOf(max));
            return this;
        }

        public Builder indexThreadPoolSize(int size) {
            properties.setProperty("thread.pool.index.size", String.valueOf(size));
            return this;
        }

        public Builder searchThreadPoolSize(int size) {
            properties.setProperty("thread.pool.search.size", String.valueOf(size));
            return this;
        }

        public Builder refreshThreadPoolSize(int size) {
            properties.setProperty("thread.pool.refresh.size", String.valueOf(size));
            return this;
        }

        public Builder enableQueryCache(boolean enable) {
            properties.setProperty("cache.query.enabled", String.valueOf(enable));
            return this;
        }

        public Builder queryCacheSizeBytes(long size) {
            properties.setProperty("cache.query.size.bytes", String.valueOf(size));
            return this;
        }

        public Builder enableMetrics(boolean enable) {
            properties.setProperty("monitoring.metrics.enabled", String.valueOf(enable));
            return this;
        }

        public Builder enableSlowLog(boolean enable) {
            properties.setProperty("monitoring.slowlog.enabled", String.valueOf(enable));
            return this;
        }

        public Builder slowLogThresholdMs(long threshold) {
            properties.setProperty("monitoring.slowlog.threshold.ms", String.valueOf(threshold));
            return this;
        }

        public IndexConfiguration build() {
            return new IndexConfiguration(properties);
        }
    }

    /**
     * Get a summary of the configuration
     */
    public String getSummary() {
        return "IndexConfiguration:\n" + "  Paths:\n" +
                "    Base: " + basePath + "\n" +
                "    Temp: " + tempPath + "\n" +
                "    Backup: " + backupPath + "\n" +
                "  Limits:\n" +
                "    Max indices: " + maxIndices + "\n" +
                "    Max shards per node: " + maxShardsPerNode + "\n" +
                "    Max index size: " + maxIndexSizeBytes / (1024.0 * 1024 * 1024) + " GB\n" +
                "    Max result window: " + maxResultWindow + "\n" +
                "  Thread Pools:\n" +
                "    Index: " + indexThreadPoolSize + "\n" +
                "    Search: " + searchThreadPoolSize + "\n" +
                "    Refresh: " + refreshThreadPoolSize + "\n" +
                "  Caching:\n" +
                "    Query cache: " + enableQueryCache + " (" + queryCacheSizeBytes / (1024.0 * 1024) + " MB)\n" +
                "    Request cache: " + enableRequestCache + " (" + requestCacheSizeBytes / (1024.0 * 1024) + " MB)\n" +
                "  Monitoring:\n" +
                "    Metrics: " + enableMetrics + "\n" +
                "    Slow log: " + enableSlowLog + " (threshold: " + slowLogThresholdMs + " ms)\n";
    }

    @Override
    public String toString() {
        return getSummary();
    }
}