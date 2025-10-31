package io.synapsedb.exception;

/**
 * Thrown when index creation fails
 */
public class IndexCreationException extends IndexException {
    private final String indexName;

    public IndexCreationException(String indexName, String message) {
        super("Failed to create index '" + indexName + "': " + message);
        this.indexName = indexName;
    }

    public IndexCreationException(String indexName, Throwable cause) {
        super("Failed to create index '" + indexName + "'", cause);
        this.indexName = indexName;
    }

    public String getIndexName() {
        return indexName;
    }
}
