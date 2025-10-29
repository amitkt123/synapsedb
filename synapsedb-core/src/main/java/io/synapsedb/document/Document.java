package io.synapsedb.document;

import java.util.*;

/**
 * Represents a document to be indexed or returned from search.
 * A document is a collection of fields (key-value pairs).
 * Example:
 * <pre>
 * Document doc = new Document("user123");
 * doc.addTextField("name", "John Doe");
 * doc.addKeywordField("email", "john@example.com");
 * doc.addNumericField("age", 30);
 * </pre>
 */
public class Document {

    private final String id;
    private final Map<String, Field> fields;
    private long version;
    private long timestamp;

    /**
     * Create a document with auto-generated ID
     */
    public Document() {
        this(UUID.randomUUID().toString());
    }

    /**
     * Create a document with specific ID
     */
    public Document(String id) {
        this.id = Objects.requireNonNull(id, "Document ID cannot be null");
        this.fields = new LinkedHashMap<>();  // Preserve insertion order
        this.version = 1;
        this.timestamp = System.currentTimeMillis();
    }

    // ============ Field Addition Methods ============

    /**
     * Add a text field - analyzed and searchable
     * Use for: Full-text content (articles, descriptions, etc.)
     */
    public Document addTextField(String name, String value) {
        validateFieldName(name);
        fields.put(name, new Field(name, value, FieldType.TEXT));
        return this;  // Fluent API
    }

    /**
     * Add a keyword field - not analyzed, exact match only
     * Use for: IDs, tags, categories, email addresses
     */
    public Document addKeywordField(String name, String value) {
        validateFieldName(name);
        fields.put(name, new Field(name, value, FieldType.KEYWORD));
        return this;
    }

    /**
     * Add a numeric field - for numbers and dates
     * Use for: Prices, ages, timestamps, quantities
     */
    public Document addNumericField(String name, Number value) {
        validateFieldName(name);
        fields.put(name, new Field(name, value, FieldType.NUMERIC));
        return this;
    }

    /**
     * Add a boolean field
     * Use for: Flags, status indicators
     */
    public Document addBooleanField(String name, boolean value) {
        validateFieldName(name);
        fields.put(name, new Field(name, value, FieldType.BOOLEAN));
        return this;
    }

    /**
     * Add a date field
     * Use for: Created date, modified date, published date
     */
    public Document addDateField(String name, Date value) {
        validateFieldName(name);
        fields.put(name, new Field(name, value.getTime(), FieldType.DATE));
        return this;
    }

    // ============ Field Retrieval Methods ============

    /**
     * Get field value as String
     */
    public String getFieldAsString(String name) {
        Field field = fields.get(name);
        return field != null ? field.getValue().toString() : null;
    }

    /**
     * Get field value as Number
     */
    public Number getFieldAsNumber(String name) {
        Field field = fields.get(name);
        if (field != null && field.getValue() instanceof Number) {
            return (Number) field.getValue();
        }
        return null;
    }

    /**
     * Get field value as Boolean
     */
    public Boolean getFieldAsBoolean(String name) {
        Field field = fields.get(name);
        if (field != null && field.getValue() instanceof Boolean) {
            return (Boolean) field.getValue();
        }
        return null;
    }

    /**
     * Get field value as Date
     */
    public Date getFieldAsDate(String name) {
        Field field = fields.get(name);
        if (field != null && field.getValue() instanceof Long) {
            return new Date((Long) field.getValue());
        }
        return null;
    }

    /**
     * Get raw field object
     */
    public Field getField(String name) {
        return fields.get(name);
    }

    /**
     * Get all fields
     */
    public Map<String, Field> getFields() {
        return Collections.unmodifiableMap(fields);
    }

    /**
     * Check if document has a specific field
     */
    public boolean hasField(String name) {
        return fields.containsKey(name);
    }

    /**
     * Remove a field
     */
    public Document removeField(String name) {
        fields.remove(name);
        return this;
    }

    // ============ Metadata Methods ============

    public String getId() {
        return id;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    // ============ Utility Methods ============

    private void validateFieldName(String name) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("Field name cannot be null or empty");
        }
    }

    /**
     * Convert to JSON-like string representation
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Document{");
        sb.append("id='").append(id).append('\'');
        sb.append(", version=").append(version);
        sb.append(", fields={");

        int i = 0;
        for (Map.Entry<String, Field> entry : fields.entrySet()) {
            if (i++ > 0) sb.append(", ");
            sb.append(entry.getKey()).append("=").append(entry.getValue().getValue());
        }

        sb.append("}}");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Document document = (Document) o;
        return Objects.equals(id, document.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}