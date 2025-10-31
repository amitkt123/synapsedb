package io.synapsedb.index;

/**
 * Represents the lifecycle state of an index in SynapseDB.
 * Indices transition through these states during their lifetime.
 * @author Amit Tiwari
 */
public enum IndexState {
    /**
     * Index is being created and is not yet available for operations
     */
    CREATING("creating"),

    /**
     * Index is open and available for read/write operations
     */
    OPEN("open"),

    /**
     * Index is closed and not available for operations but data is preserved
     */
    CLOSED("closed"),

    /**
     * Index is in the process of being deleted
     */
    DELETING("deleting"),

    /**
     * Index has encountered an error and is in a failed state
     */
    FAILED("failed"),

    /**
     * Index is being recovered from a previous state
     */
    RECOVERING("recovering");

    private final String state;

    IndexState(String state) {
        this.state = state;
    }

    public String getState() {
        return state;
    }

    /**
     * Check if the index is available for write operations
     */
    public boolean isWriteable() {
        return this == OPEN;
    }

    /**
     * Check if the index is available for read operations
     */
    public boolean isReadable() {
        return this == OPEN;
    }

    /**
     * Check if the index can be safely deleted
     */
    public boolean isDeletable() {
        return this == CLOSED || this == FAILED;
    }

    /**
     * Check if the index can be opened
     */
    public boolean canOpen() {
        return this == CLOSED || this == RECOVERING;
    }

    /**
     * Check if the index can be closed
     */
    public boolean canClose() {
        return this == OPEN;
    }

    @Override
    public String toString() {
        return state;
    }

    /**
     * Parse state from string
     */
    public static IndexState fromString(String state) {
        for (IndexState s : IndexState.values()) {
            if (s.state.equalsIgnoreCase(state)) {
                return s;
            }
        }
        throw new IllegalArgumentException("Unknown index state: " + state);
    }
}