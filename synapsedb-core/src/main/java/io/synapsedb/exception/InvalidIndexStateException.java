package io.synapsedb.exception;

import io.synapsedb.index.IndexState;

/**
 * Thrown when an index operation fails due to invalid state
 */
public class InvalidIndexStateException extends IndexException {
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
