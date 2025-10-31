package io.synapsedb.index;

/**
 * Base exception for all index-related errors in SynapseDB
 */
public class IndexException extends Exception {

    public IndexException(String message) {
        super(message);
    }

    public IndexException(String message, Throwable cause) {
        super(message, cause);
    }

    public IndexException(Throwable cause) {
        super(cause);
    }
}

/**
 * Thrown when an index is not found
 */
class IndexNotFoundException extends IndexException {
    private final String indexName;

    public IndexNotFoundException(String indexName) {
        super("Index not found: " + indexName);
        this.indexName = indexName;
    }

    public String getIndexName() {
        return indexName;
    }
}

/**
 * Thrown when attempting to create an index that already exists
 */
class IndexAlreadyExistsException extends IndexException {
    private final String indexName;

    public IndexAlreadyExistsException(String indexName) {
        super("Index already exists: " + indexName);
        this.indexName = indexName;
    }

    public String getIndexName() {
        return indexName;
    }
}

/**
 * Thrown when an index operation fails due to invalid state
 */
class InvalidIndexStateException extends IndexException {
    private final String indexName;
    private final IndexState currentState;
    private final String operation;

    public InvalidIndexStateException(String indexName, IndexState currentState, String operation) {
        super(String.format("Cannot perform operation '%s' on index '%s' in state '%s'",
                operation, indexName, currentState));
        this.indexName = indexName;
        this.currentState = currentState;
        this.operation = operation;
    }

    public String getIndexName() {
        return indexName;
    }

    public IndexState getCurrentState() {
        return currentState;
    }

    public String getOperation() {
        return operation;
    }
}

/**
 * Thrown when index creation fails
 */
class IndexCreationException extends IndexException {
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

/**
 * Thrown when index deletion fails
 */
class IndexDeletionException extends IndexException {
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