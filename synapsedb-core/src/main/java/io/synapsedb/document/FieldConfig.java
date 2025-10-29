package io.synapsedb.document;

/**
 * Configuration for how a field should be indexed and stored
 */
public class FieldConfig {
    private boolean stored;           // Should be stored?
    private boolean indexed;          // Should be indexed?
    private boolean tokenized;        // Should be analyzed/tokenized?
    private FieldType type;           // Data type

    public enum FieldType {
        TEXT,           // Full-text searchable
        KEYWORD,        // Exact match, not tokenized
        LONG,           // Numeric long
        INTEGER,        // Numeric int
        DOUBLE,         // Numeric double
        FLOAT,          // Numeric float
        BOOLEAN,        // Boolean
        DATE,           // Date/timestamp
        BINARY,         // Binary data
        OBJECT          // Nested object
    }

    private FieldConfig() {}

    public static FieldConfig defaults() {
        return new FieldConfig()
                .setStored(true)
                .setIndexed(true)
                .setTokenized(true)
                .setType(FieldType.TEXT);
    }

    public static FieldConfig text() {
        return defaults()
                .setType(FieldType.TEXT)
                .setTokenized(true);
    }

    public static FieldConfig keyword() {
        return defaults()
                .setType(FieldType.KEYWORD)
                .setTokenized(false);
    }

    public static FieldConfig number(FieldType type) {
        return defaults()
                .setType(type)
                .setTokenized(false);
    }

    public static FieldConfig storedOnly() {
        return new FieldConfig()
                .setStored(true)
                .setIndexed(false);
    }

    public static FieldConfig indexedOnly() {
        return new FieldConfig()
                .setStored(false)
                .setIndexed(true)
                .setTokenized(true)
                .setType(FieldType.TEXT);
    }

    // Fluent setters
    public FieldConfig setStored(boolean stored) {
        this.stored = stored;
        return this;
    }

    public FieldConfig setIndexed(boolean indexed) {
        this.indexed = indexed;
        return this;
    }

    public FieldConfig setTokenized(boolean tokenized) {
        this.tokenized = tokenized;
        return this;
    }

    public FieldConfig setType(FieldType type) {
        this.type = type;
        return this;
    }

    // Getters
    public boolean isStored() { return stored; }
    public boolean isIndexed() { return indexed; }
    public boolean isTokenized() { return tokenized; }
    public FieldType getType() { return type; }
}
