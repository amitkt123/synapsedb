package io.synapsedb.core.document;

import java.util.Objects;

/**
 * Configuration for a document field (stored, indexed, tokenized, type)
 * @author Amit Tiwari
 * Configuration for how a field should be indexed and stored
 */
public class FieldConfig {
    private final boolean stored;
    private final boolean indexed;
    private final boolean tokenized;
    private final FieldType type;

    private FieldConfig(Builder builder) {
        this.stored = builder.stored;
        this.indexed = builder.indexed;
        this.tokenized = builder.tokenized;
        this.type = builder.type;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private boolean stored = true;
        private boolean indexed = true;
        private boolean tokenized = true;
        private FieldType type = FieldType.TEXT;

        public Builder stored(boolean stored) {
            this.stored = stored;
            return this;
        }

        public Builder indexed(boolean indexed) {
            this.indexed = indexed;
            return this;
        }

        public Builder tokenized(boolean tokenized) {
            this.tokenized = tokenized;
            return this;
        }

        public Builder type(FieldType type) {
            this.type = Objects.requireNonNull(type, "FieldType cannot be null");
            // Auto-adjust tokenized flag if type doesn't support tokenization
            if (!type.allowsTokenization()) {
                this.tokenized = false;
            }
            return this;
        }

        public FieldConfig build() {
            validate();
            return new FieldConfig(this);
        }

        private void validate() {
            // 1. Tokenization only for types that allow it (TEXT, KEYWORD)
            if (tokenized && !type.allowsTokenization()) {
                throw new IllegalStateException(
                        "Tokenization is only allowed for TEXT or KEYWORD fields");
            }

            // 2. Tokenization without indexing is also suspicious
            if (tokenized && !indexed) {
                throw new IllegalStateException(
                        "Tokenization is only useful when the field is indexed");
            }

            // 3. Useless field guard
            if (!indexed && !stored) {
                throw new IllegalStateException(
                        "Field must be either indexed or stored (or both)");
            }
        }

    }

    // Getters only
    public boolean isStored() { return stored; }
    public boolean isIndexed() { return indexed; }
    public boolean isTokenized() { return tokenized; }
    public FieldType getType() { return type; }
}
