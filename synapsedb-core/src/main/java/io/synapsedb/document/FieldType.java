package io.synapsedb.document;

/**
 * Enumeration of supported field types in SynapseDB.
 * Each type determines how the field is indexed and searched.
 */
public enum FieldType {

    /**
     * Full-text searchable field.
     * Analyzed (tokenized, lowercased, stemmed).
     * Use for: Article content, descriptions, reviews.
     */
    TEXT,

    /**
     * Exact-match field, not analyzed.
     * Use for: IDs, tags, categories, email addresses.
     */
    KEYWORD,

    /**
     * Numeric field (integers, longs, floats, doubles).
     * Supports range queries.
     * Use for: Prices, ages, scores, quantities.
     */
    NUMERIC,

    /**
     * Boolean field (true/false).
     * Use for: Flags, status indicators.
     */
    BOOLEAN,

    /**
     * Date/timestamp field.
     * Stored as long (milliseconds since epoch).
     * Supports range queries.
     * Use for: Created date, modified date, published date.
     */
    DATE,

    /**
     * Geographic point field (lat, lon).
     * Supports geo-distance queries.
     * Use for: Locations, addresses.
     * (Phase 2 feature)
     */
    GEO_POINT,

    /**
     * Binary data field.
     * Not indexed, only stored.
     * Use for: Images, PDFs (as byte arrays).
     * (Phase 2 feature)
     */
    BINARY
}