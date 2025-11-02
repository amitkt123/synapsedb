package io.synapsedb.query.validation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Result of query validation.
 * Contains any errors or warnings found during validation.
 *
 * This acts as the "safety inspector's report" - telling you
 * what's wrong before you waste resources executing a bad query.
 *
 * @author Amit Tiwari
 */
public class ValidationResult {

    private final List<ValidationError> errors;
    private final List<ValidationError> warnings;
    private final boolean valid;

    private ValidationResult(List<ValidationError> errors, List<ValidationError> warnings) {
        this.errors = errors;
        this.warnings = warnings;
        this.valid = errors.isEmpty();
    }

    /**
     * Create a successful validation result (no errors)
     */
    public static ValidationResult success() {
        return new ValidationResult(Collections.emptyList(), Collections.emptyList());
    }

    /**
     * Create a failed validation result with errors
     */
    public static ValidationResult failure(List<ValidationError> errors) {
        return new ValidationResult(errors, Collections.emptyList());
    }

    /**
     * Create a validation result with both errors and warnings
     */
    public static ValidationResult of(List<ValidationError> errors, List<ValidationError> warnings) {
        return new ValidationResult(errors, warnings);
    }

    // Getters

    public boolean isValid() {
        return valid;
    }

    public List<ValidationError> getErrors() {
        return Collections.unmodifiableList(errors);
    }

    public List<ValidationError> getWarnings() {
        return Collections.unmodifiableList(warnings);
    }

    public boolean hasWarnings() {
        return !warnings.isEmpty();
    }

    public int getErrorCount() {
        return errors.size();
    }

    public int getWarningCount() {
        return warnings.size();
    }

    /**
     * Get all error messages as a single string
     */
    public String getErrorMessage() {
        if (errors.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (ValidationError error : errors) {
            sb.append(error.getMessage()).append("\n");
        }
        return sb.toString().trim();
    }

    /**
     * Get all warning messages as a single string
     */
    public String getWarningMessage() {
        if (warnings.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (ValidationError warning : warnings) {
            sb.append(warning.getMessage()).append("\n");
        }
        return sb.toString().trim();
    }

    @Override
    public String toString() {
        if (valid && warnings.isEmpty()) {
            return "ValidationResult{valid=true}";
        }
        return "ValidationResult{" +
                "valid=" + valid +
                ", errors=" + errors.size() +
                ", warnings=" + warnings.size() +
                '}';
    }

    /**
     * Builder for creating ValidationResult
     */
    public static class Builder {
        private final List<ValidationError> errors = new ArrayList<>();
        private final List<ValidationError> warnings = new ArrayList<>();

        public Builder addError(ValidationError error) {
            this.errors.add(error);
            return this;
        }

        public Builder addError(String field, String message) {
            return addError(ValidationError.error(field, message));
        }

        public Builder addWarning(ValidationError warning) {
            this.warnings.add(warning);
            return this;
        }

        public Builder addWarning(String field, String message) {
            return addWarning(ValidationError.warning(field, message));
        }

        public ValidationResult build() {
            return ValidationResult.of(errors, warnings);
        }
    }
}

