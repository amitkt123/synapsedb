package io.synapsedb.core.query.validation;

/**
 * Represents a validation error or warning.
 * Provides details about what went wrong during query validation.
 *
 * @author Amit Tiwari
 */
public class ValidationError {

    private final String field;
    private final String message;
    private final ValidationSeverity severity;
    private final String errorCode;

    /**
     * Severity level of validation issues
     */
    public enum ValidationSeverity {
        ERROR,      // Query cannot be executed
        WARNING,    // Query can execute but might have issues
        INFO        // Informational message
    }

    private ValidationError(String field, String message, ValidationSeverity severity, String errorCode) {
        this.field = field;
        this.message = message;
        this.severity = severity;
        this.errorCode = errorCode;
    }

    // Factory methods for convenience

    public static ValidationError error(String field, String message) {
        return new ValidationError(field, message, ValidationSeverity.ERROR, null);
    }

    public static ValidationError error(String field, String message, String errorCode) {
        return new ValidationError(field, message, ValidationSeverity.ERROR, errorCode);
    }

    public static ValidationError warning(String field, String message) {
        return new ValidationError(field, message, ValidationSeverity.WARNING, null);
    }

    public static ValidationError info(String field, String message) {
        return new ValidationError(field, message, ValidationSeverity.INFO, null);
    }

    // Getters

    public String getField() {
        return field;
    }

    public String getMessage() {
        return message;
    }

    public ValidationSeverity getSeverity() {
        return severity;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public boolean isError() {
        return severity == ValidationSeverity.ERROR;
    }

    public boolean isWarning() {
        return severity == ValidationSeverity.WARNING;
    }

    @Override
    public String toString() {
        return String.format("[%s] %s: %s", severity, field != null ? field : "query", message);
    }
}
