package io.synapsedb.exception;

/**
 * Thrown when index deletion fails
 */
public class IndexDeletionException extends IndexException {
    private final String indexName;

    public IndexDeletionException(String indexName, String message) {
        super("Failed to delete index '" + indexName + "': " + message);
        this.indexName = indexName;
    }

    public IndexDeletionException(String indexName, Throwable cause) {
        super("Failed to delete index '" + indexName + "'", cause);
        this.indexName = indexName;
    }

    public String getIndexName() {
        return indexName;
    }
}
