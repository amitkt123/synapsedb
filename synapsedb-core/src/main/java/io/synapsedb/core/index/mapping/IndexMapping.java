package io.synapsedb.core.index.mapping;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Immutable index mapping: field name -> FieldMapping + dynamic policy.
 * Start small; extend with nested objects and runtime serialization later.
 */
public final class IndexMapping {

    private static final Pattern FIELD_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_.]*");

    private final Map<String, FieldMapping> fields;
    private final DynamicMapping dynamic;

    private IndexMapping(Map<String, FieldMapping> fields, DynamicMapping dynamic) {
        this.fields = Collections.unmodifiableMap(new LinkedHashMap<>(fields));
        this.dynamic = Objects.requireNonNull(dynamic, "dynamic");
    }

    public static Builder builder() { return new Builder(); }

    public Map<String, FieldMapping> fields() { return fields; }
    public Optional<FieldMapping> field(String name) { return Optional.ofNullable(fields.get(name)); }
    public boolean hasField(String name) { return fields.containsKey(name); }
    public DynamicMapping dynamic() { return dynamic; }

    /**
     * Merge another mapping. If a field exists in both:
     * - if `failOnConflict` and definitions differ -> throw
     * - else keep current (do not overwrite)
     */
    public IndexMapping merge(IndexMapping other, boolean failOnConflict) {
        Objects.requireNonNull(other, "other");
        Builder b = toBuilder();
        for (Map.Entry<String, FieldMapping> e : other.fields.entrySet()) {
            String name = e.getKey();
            FieldMapping incoming = e.getValue();
            FieldMapping existing = fields.get(name);
            if (existing == null) {
                if (dynamic.rejectUnknownFields()) {
                    throw new IllegalStateException("Unknown field '" + name + "' not allowed under STRICT dynamic policy");
                }
                b.putField(name, incoming);
            } else if (existing.conflictsWith(incoming)) {
                if (failOnConflict) {
                    throw new IllegalArgumentException("Conflicting mapping for field: " + name);
                }
                // keep existing
            }
        }
        // Keep current dynamic policy; change only via builder if desired.
        b.dynamic(this.dynamic);
        return b.build();
    }

    public Builder toBuilder() {
        Builder b = new Builder();
        b.fields.putAll(this.fields);
        b.dynamic(this.dynamic);
        return b;
    }

    @Override
    public String toString() { return "IndexMapping{fields=" + fields.keySet() + ", dynamic=" + dynamic + "}"; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof IndexMapping that)) return false;
        return fields.equals(that.fields) && dynamic.equals(that.dynamic);
    }

    @Override
    public int hashCode() { return Objects.hash(fields, dynamic); }

    public static final class Builder {
        private final Map<String, FieldMapping> fields = new LinkedHashMap<>();
        private DynamicMapping dynamic = DynamicMapping.enabled();

        public Builder putField(String name, FieldMapping mapping) {
            validateName(name);
            Objects.requireNonNull(mapping, "mapping");
            fields.put(name, mapping);
            return this;
        }

        public Builder removeField(String name) {
            Objects.requireNonNull(name, "name");
            fields.remove(name);
            return this;
        }

        public Builder dynamic(DynamicMapping dynamic) {
            this.dynamic = Objects.requireNonNull(dynamic, "dynamic");
            return this;
        }

        public IndexMapping build() {
            // Basic sanity: no nulls and valid names already enforced in putField.
            return new IndexMapping(fields, dynamic);
        }

        private static void validateName(String name) {
            Objects.requireNonNull(name, "name");
            String n = name.trim();
            if (n.isEmpty() || !FIELD_NAME.matcher(n).matches()) {
                throw new IllegalArgumentException("Invalid field name: " + name);
            }
        }
    }
}
