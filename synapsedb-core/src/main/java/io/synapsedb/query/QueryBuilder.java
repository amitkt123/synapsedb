package io.synapsedb.query;

import io.synapsedb.query.validation.ValidationResult;
import org.apache.lucene.search.Query;

import java.util.HashMap;
import java.util.Map;

/**
 * Abstract base class for all query builders in SynapseDB.
 * Provides a fluent API for building Lucene queries with validation and serialization support.
 * This is the foundation of the Query Framework - an abstraction layer that makes
 * query building simple, safe, and intuitive.
 *
 * @author Amit Tiwari
 */
public abstract class QueryBuilder {

    protected QueryContext context;
    protected Map<String, Object> metadata;
    protected float boost = 1.0f;

    public QueryBuilder() {
        this.metadata = new HashMap<>();
    }

    /**
     * Convert this query builder to a Lucene Query.
     * This is where the translation from SynapseDB → Lucene happens.
     *
     * @return The Lucene query representation
     */
    public abstract Query toLuceneQuery();

    /**
     * Validate this query before execution.
     * Catches errors early before expensive query execution.
     *
     * @return ValidationResult containing any errors or warnings
     */
    public abstract ValidationResult validate();

    /**
     * Get a human-readable string representation of this query.
     *
     * @return String representation
     */
    public abstract String toString();

    /**
     * Convert this query to JSON format for serialization.
     * Useful for REST APIs, logging, and debugging.
     *
     * @return JSON string representation
     */
    public abstract String toJson();

    /**
     * Get the query type name (e.g., "term", "match", "bool")
     *
     * @return Query type name
     */
    public abstract String getQueryType();

    // Fluent setters

    /**
     * Set the query context for additional metadata and settings.
     *
     * @param context Query context
     * @return This builder for chaining
     */
    public QueryBuilder setContext(QueryContext context) {
        this.context = context;
        return this;
    }

    /**
     * Set a boost value for this query (affects scoring).
     *
     * @param boost Boost value (default: 1.0)
     * @return This builder for chaining
     */
    public QueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * Add custom metadata to this query.
     *
     * @param key Metadata key
     * @param value Metadata value
     * @return This builder for chaining
     */
    public QueryBuilder addMetadata(String key, Object value) {
        this.metadata.put(key, value);
        return this;
    }

    // Getters

    public QueryContext getContext() {
        return context;
    }

    public float getBoost() {
        return boost;
    }

    public Map<String, Object> getMetadata() {
        return new HashMap<>(metadata);
    }

    /**
     * Helper method to check if the query is valid.
     * Convenience method that calls validate() and checks the result.
     *
     * @return true if valid, false otherwise
     */
    public boolean isValid() {
        return validate().isValid();
    }
}

