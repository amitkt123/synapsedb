package io.synapsedb.index.mapping;

/**
 * Fluent builder for creating IndexMapping instances with a convenient API.
 * Provides methods for adding fields of different types with common configurations.
 * Example usage:
 * <pre>
 * IndexMapping mapping = MappingBuilder.create()
 *     .addTextField("title", true, "standard")
 *     .addKeywordField("category", true)
 *     .addLongField("timestamp", true)
 *     .addDoubleField("price", false)
 *     .dynamicMapping(DynamicMapping.strict())
 *     .build();
 * </pre>
 *
 * @author Amit Tiwari
 */
public class MappingBuilder {

    private final IndexMapping.Builder internalBuilder;
    private FieldBuilder currentField;

    private MappingBuilder() {
        this.internalBuilder = IndexMapping.builder();
    }

    /**
     * Create a new MappingBuilder instance.
     */
    public static MappingBuilder create() {
        return new MappingBuilder();
    }

    /**
     * Start building a new field with the given name and type.
     * Allows for fine-grained control over field configuration.
     *
     * @param name field name
     * @param type field type
     * @return FieldBuilder for configuring the field
     */
    public FieldBuilder field(String name, FieldMapping.FieldType type) {
        finishCurrentField();
        this.currentField = new FieldBuilder(this, name, type);
        return this.currentField;
    }

    // ==================== Convenience Methods for Common Field Types ====================

    /**
     * Add a TEXT field (full-text searchable).
     *
     * @param name field name
     * @param stored whether to store the field value
     * @param analyzer analyzer name (e.g., "standard", "english")
     * @return this builder
     */
    public MappingBuilder addTextField(String name, boolean stored, String analyzer) {
        finishCurrentField();
        FieldMapping mapping = FieldMapping.builder(FieldMapping.FieldType.TEXT)
            .stored(stored)
            .indexed(true)
            .analyzer(analyzer)
            .build();
        internalBuilder.putField(name, mapping);
        return this;
    }

    /**
     * Add a TEXT field with default analyzer.
     *
     * @param name field name
     * @param stored whether to store the field value
     * @return this builder
     */
    public MappingBuilder addTextField(String name, boolean stored) {
        return addTextField(name, stored, null);
    }

    /**
     * Add a TEXT field with default settings (stored, no specific analyzer).
     *
     * @param name field name
     * @return this builder
     */
    public MappingBuilder addTextField(String name) {
        return addTextField(name, true, null);
    }

    /**
     * Add a KEYWORD field (exact matching, not analyzed).
     *
     * @param name field name
     * @param stored whether to store the field value
     * @param docValues whether to enable doc values for sorting/aggregations
     * @return this builder
     */
    public MappingBuilder addKeywordField(String name, boolean stored, boolean docValues) {
        finishCurrentField();
        FieldMapping mapping = FieldMapping.builder(FieldMapping.FieldType.KEYWORD)
            .stored(stored)
            .indexed(true)
            .docValues(docValues)
            .build();
        internalBuilder.putField(name, mapping);
        return this;
    }

    /**
     * Add a KEYWORD field with default settings (stored, with doc values).
     *
     * @param name field name
     * @param stored whether to store the field value
     * @return this builder
     */
    public MappingBuilder addKeywordField(String name, boolean stored) {
        return addKeywordField(name, stored, true);
    }

    /**
     * Add a KEYWORD field with default settings (stored, with doc values).
     *
     * @param name field name
     * @return this builder
     */
    public MappingBuilder addKeywordField(String name) {
        return addKeywordField(name, true, true);
    }

    /**
     * Add a LONG field (64-bit integer).
     *
     * @param name field name
     * @param stored whether to store the field value
     * @param docValues whether to enable doc values for sorting/aggregations
     * @return this builder
     */
    public MappingBuilder addLongField(String name, boolean stored, boolean docValues) {
        finishCurrentField();
        FieldMapping mapping = FieldMapping.builder(FieldMapping.FieldType.LONG)
            .stored(stored)
            .indexed(true)
            .docValues(docValues)
            .build();
        internalBuilder.putField(name, mapping);
        return this;
    }

    /**
     * Add a LONG field with default settings.
     *
     * @param name field name
     * @param docValues whether to enable doc values
     * @return this builder
     */
    public MappingBuilder addLongField(String name, boolean docValues) {
        return addLongField(name, false, docValues);
    }

    /**
     * Add a LONG field with doc values enabled.
     *
     * @param name field name
     * @return this builder
     */
    public MappingBuilder addLongField(String name) {
        return addLongField(name, false, true);
    }

    /**
     * Add a DOUBLE field (64-bit floating point).
     *
     * @param name field name
     * @param stored whether to store the field value
     * @param docValues whether to enable doc values for sorting/aggregations
     * @return this builder
     */
    public MappingBuilder addDoubleField(String name, boolean stored, boolean docValues) {
        finishCurrentField();
        FieldMapping mapping = FieldMapping.builder(FieldMapping.FieldType.DOUBLE)
            .stored(stored)
            .indexed(true)
            .docValues(docValues)
            .build();
        internalBuilder.putField(name, mapping);
        return this;
    }

    /**
     * Add a DOUBLE field with default settings.
     *
     * @param name field name
     * @param docValues whether to enable doc values
     * @return this builder
     */
    public MappingBuilder addDoubleField(String name, boolean docValues) {
        return addDoubleField(name, false, docValues);
    }

    /**
     * Add a DOUBLE field with doc values enabled.
     *
     * @param name field name
     * @return this builder
     */
    public MappingBuilder addDoubleField(String name) {
        return addDoubleField(name, false, true);
    }

    /**
     * Add a DATE field.
     *
     * @param name field name
     * @param stored whether to store the field value
     * @param docValues whether to enable doc values for sorting/aggregations
     * @return this builder
     */
    public MappingBuilder addDateField(String name, boolean stored, boolean docValues) {
        finishCurrentField();
        FieldMapping mapping = FieldMapping.builder(FieldMapping.FieldType.DATE)
            .stored(stored)
            .indexed(true)
            .docValues(docValues)
            .build();
        internalBuilder.putField(name, mapping);
        return this;
    }

    /**
     * Add a DATE field with default settings.
     *
     * @param name field name
     * @param docValues whether to enable doc values
     * @return this builder
     */
    public MappingBuilder addDateField(String name, boolean docValues) {
        return addDateField(name, false, docValues);
    }

    /**
     * Add a DATE field with doc values enabled.
     *
     * @param name field name
     * @return this builder
     */
    public MappingBuilder addDateField(String name) {
        return addDateField(name, false, true);
    }

    /**
     * Add a BOOLEAN field.
     *
     * @param name field name
     * @param stored whether to store the field value
     * @param docValues whether to enable doc values for sorting/aggregations
     * @return this builder
     */
    public MappingBuilder addBooleanField(String name, boolean stored, boolean docValues) {
        finishCurrentField();
        FieldMapping mapping = FieldMapping.builder(FieldMapping.FieldType.BOOLEAN)
            .stored(stored)
            .indexed(true)
            .docValues(docValues)
            .build();
        internalBuilder.putField(name, mapping);
        return this;
    }

    /**
     * Add a BOOLEAN field with default settings.
     *
     * @param name field name
     * @param docValues whether to enable doc values
     * @return this builder
     */
    public MappingBuilder addBooleanField(String name, boolean docValues) {
        return addBooleanField(name, false, docValues);
    }

    /**
     * Add a BOOLEAN field with doc values enabled.
     *
     * @param name field name
     * @return this builder
     */
    public MappingBuilder addBooleanField(String name) {
        return addBooleanField(name, false, true);
    }

    /**
     * Add a pre-built FieldMapping directly.
     *
     * @param name field name
     * @param mapping the field mapping
     * @return this builder
     */
    public MappingBuilder addField(String name, FieldMapping mapping) {
        finishCurrentField();
        internalBuilder.putField(name, mapping);
        return this;
    }

    /**
     * Remove a field from the mapping.
     *
     * @param name field name to remove
     * @return this builder
     */
    public MappingBuilder removeField(String name) {
        finishCurrentField();
        internalBuilder.removeField(name);
        return this;
    }

    /**
     * Set the dynamic mapping policy.
     *
     * @param dynamic the dynamic mapping policy
     * @return this builder
     */
    public MappingBuilder dynamicMapping(DynamicMapping dynamic) {
        finishCurrentField();
        internalBuilder.dynamic(dynamic);
        return this;
    }

    /**
     * Enable dynamic mapping (allow unknown fields).
     *
     * @return this builder
     */
    public MappingBuilder dynamicEnabled() {
        return dynamicMapping(DynamicMapping.enabled());
    }

    /**
     * Disable dynamic mapping (ignore unknown fields).
     *
     * @return this builder
     */
    public MappingBuilder dynamicDisabled() {
        return dynamicMapping(DynamicMapping.disabled());
    }

    /**
     * Set strict dynamic mapping (reject unknown fields).
     *
     * @return this builder
     */
    public MappingBuilder dynamicStrict() {
        return dynamicMapping(DynamicMapping.strict());
    }

    /**
     * Build the final IndexMapping.
     *
     * @return the constructed IndexMapping
     */
    public IndexMapping build() {
        finishCurrentField();
        return internalBuilder.build();
    }

    /**
     * Finish building the current field if one is in progress.
     */
    private void finishCurrentField() {
        if (currentField != null) {
            currentField.finishField();
            currentField = null;
        }
    }

    // ==================== Nested FieldBuilder for Fine-Grained Control ====================

    /**
     * Builder for configuring individual fields with full control over all options.
     */
    public static class FieldBuilder {

        private final MappingBuilder parent;
        private final String fieldName;
        private final FieldMapping.Builder fieldMappingBuilder;

        private FieldBuilder(MappingBuilder parent, String fieldName, FieldMapping.FieldType type) {
            this.parent = parent;
            this.fieldName = fieldName;
            this.fieldMappingBuilder = FieldMapping.builder(type);
        }

        /**
         * Set whether the field should be stored.
         *
         * @param stored true to store the field value
         * @return this FieldBuilder
         */
        public FieldBuilder stored(boolean stored) {
            fieldMappingBuilder.stored(stored);
            return this;
        }

        /**
         * Set whether the field should be indexed.
         *
         * @param indexed true to index the field
         * @return this FieldBuilder
         */
        public FieldBuilder indexed(boolean indexed) {
            fieldMappingBuilder.indexed(indexed);
            return this;
        }

        /**
         * Set whether the field should have doc values.
         *
         * @param docValues true to enable doc values
         * @return this FieldBuilder
         */
        public FieldBuilder docValues(boolean docValues) {
            fieldMappingBuilder.docValues(docValues);
            return this;
        }

        /**
         * Set the analyzer for TEXT fields.
         *
         * @param analyzer analyzer name
         * @return this FieldBuilder
         */
        public FieldBuilder analyzer(String analyzer) {
            fieldMappingBuilder.analyzer(analyzer);
            return this;
        }

        /**
         * Finish configuring this field and return to the parent MappingBuilder.
         *
         * @return the parent MappingBuilder
         */
        public MappingBuilder end() {
            finishField();
            return parent;
        }

        /**
         * Finish configuring this field and start a new field.
         *
         * @param name the new field name
         * @param type the new field type
         * @return a new FieldBuilder for the next field
         */
        public FieldBuilder field(String name, FieldMapping.FieldType type) {
            finishField();
            return parent.field(name, type);
        }

        /**
         * Finish configuring this field and build the final mapping.
         *
         * @return the constructed IndexMapping
         */
        public IndexMapping build() {
            finishField();
            return parent.build();
        }

        /**
         * Internal method to finalize the field and add it to the mapping.
         */
        void finishField() {
            FieldMapping mapping = fieldMappingBuilder.build();
            parent.internalBuilder.putField(fieldName, mapping);
        }
    }
}
