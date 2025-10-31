package io.synapsedb.exception;

/**
 * Base exception for all index-related errors in SynapseDB
 * @author Amit Tiwari
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

