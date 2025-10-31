package io.synapsedb.exception;

/**
 * Thrown when attempting to create an index that already exists
 */
public class IndexAlreadyExistsException extends IndexException {
    private final String indexName;

    public IndexAlreadyExistsException(String indexName) {
        super("Index already exists: " + indexName);
        this.indexName = indexName;
    }

    public String getIndexName() {
        return indexName;
    }
}
