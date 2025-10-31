package io.synapsedb.exception;

/**
 * Thrown when an index is not found
 */
public class IndexNotFoundException extends IndexException {
    private final String indexName;

    public IndexNotFoundException(String indexName) {
        super("Index not found: " + indexName);
        this.indexName = indexName;
    }

    public String getIndexName() {
        return indexName;
    }
}
