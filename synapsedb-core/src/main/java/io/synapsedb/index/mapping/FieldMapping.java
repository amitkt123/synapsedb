package io.synapsedb.index.mapping;

import java.util.Objects;
import java.util.Optional;

/**
 * Minimal field mapping.
 * Extend later with analyzers/format/nested/sub-fields as needed.
 */
public final class FieldMapping {

    public enum FieldType { KEYWORD, TEXT, LONG, DOUBLE, DATE, BOOLEAN }

    private final FieldType type;
    private final boolean stored;
    private final boolean indexed;
    private final boolean docValues;
    private final String analyzer; // only meaningful for TEXT; optional

    private FieldMapping(Builder b) {
        this.type = Objects.requireNonNull(b.type, "type");
        this.stored = b.stored;
        this.indexed = b.indexed;
        this.docValues = b.docValues;
        this.analyzer = b.analyzer;
        if (type != FieldType.TEXT && b.analyzer != null) {
            throw new IllegalArgumentException("analyzer is only valid for TEXT type");
        }
    }

    public static Builder builder(FieldType type) { return new Builder(type); }

    public FieldType type() { return type; }
    public boolean stored() { return stored; }
    public boolean indexed() { return indexed; }
    public boolean docValues() { return docValues; }
    public Optional<String> analyzer() { return Optional.ofNullable(analyzer); }

    // Two mappings conflict if any semantically relevant attribute differs.
    public boolean conflictsWith(FieldMapping other) {
        if (other == null) return false;
        if (this.type != other.type) return true;
        if (this.stored != other.stored) return true;
        if (this.indexed != other.indexed) return true;
        if (this.docValues != other.docValues) return true;
        return !Objects.equals(this.analyzer, other.analyzer);
    }

    public static final class Builder {
        private final FieldType type;
        private boolean stored;
        private boolean indexed = true;
        private boolean docValues;
        private String analyzer;

        public Builder(FieldType type) {
            this.type = Objects.requireNonNull(type, "type");
        }

        public Builder stored(boolean v) { this.stored = v; return this; }
        public Builder indexed(boolean v) { this.indexed = v; return this; }
        public Builder docValues(boolean v) { this.docValues = v; return this; }
        public Builder analyzer(String name) { this.analyzer = name; return this; }

        public FieldMapping build() { return new FieldMapping(this); }
    }

    @Override
    public String toString() {
        return "FieldMapping{type=" + type + ", stored=" + stored + ", indexed=" + indexed +
                ", docValues=" + docValues + ", analyzer=" + analyzer + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FieldMapping that)) return false;
        return stored == that.stored &&
                indexed == that.indexed &&
                docValues == that.docValues &&
                type == that.type &&
                Objects.equals(analyzer, that.analyzer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, stored, indexed, docValues, analyzer);
    }
}
