package io.synapsedb.query;

import java.util.HashMap;
import java.util.Map;

/**
 * Context and metadata for query execution.
 * Tracks who, when, where, and how queries are executed.
 * @author Amit Tiwari
 */
public class QueryContext {

    private String indexName;
    private long timestamp;
    private Map<String, Object> metadata;
    private QuerySource source;
    private String userId;
    private String queryId;

    // Query execution hints
    private int timeout = -1; // -1 means no timeout
    private boolean allowExpensiveQueries = true;
    private CachePolicy cachePolicy = CachePolicy.DEFAULT;
    private boolean explain = false; // Return scoring explanation

    /**
     * Source of the query execution
     */
    public enum QuerySource {
        API,            // REST API request
        CLI,            // Command line
        INTERNAL,       // Internal system query
        SCHEDULED,      // Scheduled/batch query
        TEST            // Test query
    }

    /**
     * Cache policy for query results
     */
    public enum CachePolicy {
        DEFAULT,        // Use default caching behavior
        ALWAYS,         // Always cache results
        NEVER,          // Never cache results
        CONDITIONAL     // Cache based on query characteristics
    }

    public QueryContext() {
        this.timestamp = System.currentTimeMillis();
        this.metadata = new HashMap<>();
    }

    public QueryContext(String indexName) {
        this();
        this.indexName = indexName;
    }

    // Fluent setters

    public QueryContext setIndexName(String indexName) {
        this.indexName = indexName;
        return this;
    }

    public QueryContext setUserId(String userId) {
        this.userId = userId;
        return this;
    }

    public QueryContext setQueryId(String queryId) {
        this.queryId = queryId;
        return this;
    }

    public QueryContext setSource(QuerySource source) {
        this.source = source;
        return this;
    }

    public QueryContext setTimeout(int timeout) {
        this.timeout = timeout;
        return this;
    }

    public QueryContext setAllowExpensiveQueries(boolean allow) {
        this.allowExpensiveQueries = allow;
        return this;
    }

    public QueryContext setCachePolicy(CachePolicy policy) {
        this.cachePolicy = policy;
        return this;
    }

    public QueryContext setExplain(boolean explain) {
        this.explain = explain;
        return this;
    }

    public QueryContext addMetadata(String key, Object value) {
        this.metadata.put(key, value);
        return this;
    }

    // Getters

    public String getIndexName() {
        return indexName;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Map<String, Object> getMetadata() {
        return new HashMap<>(metadata);
    }

    public QuerySource getSource() {
        return source;
    }

    public String getUserId() {
        return userId;
    }

    public String getQueryId() {
        return queryId;
    }

    public int getTimeout() {
        return timeout;
    }

    public boolean isAllowExpensiveQueries() {
        return allowExpensiveQueries;
    }

    public CachePolicy getCachePolicy() {
        return cachePolicy;
    }

    public boolean isExplain() {
        return explain;
    }

    @Override
    public String toString() {
        return "QueryContext{" +
                "indexName='" + indexName + '\'' +
                ", timestamp=" + timestamp +
                ", source=" + source +
                ", userId='" + userId + '\'' +
                ", queryId='" + queryId + '\'' +
                ", timeout=" + timeout +
                '}';
    }
}

