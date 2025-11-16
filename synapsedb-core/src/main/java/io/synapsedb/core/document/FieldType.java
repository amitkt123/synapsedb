package io.synapsedb.core.document;

/**
 * Author: Amit Tiwari
 * Date: 16/11/25
 * Time: 10:24 am
 */

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