package io.synapsedb.document;

import java.util.*;

/**
 * SynapseDB's abstraction over Lucene Document
 * Provides a cleaner, more flexible API for document manipulation
 */
public class Document {
    private String id;  // Mandatory unique identifier
    private final Map<String, List<Object>> fields;  // Multi-valued fields support
    private final Map<String, FieldConfig> fieldConfigs;  // Field metadata

    public Document() {
        this.fields = new LinkedHashMap<>();  // Preserve insertion order
        this.fieldConfigs = new HashMap<>();
    }

    public Document(String id) {
        this();
        this.id = id;
    }

    // Add single value
    public Document addField(String name, Object value) {
        return addField(name, value, FieldConfig.defaults());
    }

    public Document addField(String name, Object value, FieldConfig config) {
        fields.computeIfAbsent(name, k -> new ArrayList<>()).add(value);
        fieldConfigs.put(name, config);
        return this;  // Fluent API
    }

    // Add multiple values
    public Document addFields(String name, List<Object> values) {
        return addFields(name, values, FieldConfig.defaults());
    }

    public Document addFields(String name, List<Object> values, FieldConfig config) {
        fields.computeIfAbsent(name, k -> new ArrayList<>()).addAll(values);
        fieldConfigs.put(name, config);
        return this;
    }

    // Getters
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Object getField(String name) {
        List<Object> values = fields.get(name);
        return values != null && !values.isEmpty() ? values.get(0) : null;
    }

    public List<Object> getFields(String name) {
        return fields.getOrDefault(name, Collections.emptyList());
    }

    public Set<String> getFieldNames() {
        return fields.keySet();
    }

    public Map<String, List<Object>> getAllFields() {
        return new HashMap<>(fields);
    }

    public FieldConfig getFieldConfig(String name) {
        return fieldConfigs.getOrDefault(name, FieldConfig.defaults());
    }

    public boolean hasField(String name) {
        return fields.containsKey(name);
    }

    public int getFieldCount() {
        return fields.size();
    }

    @Override
    public String toString() {
        return "SynapseDoc{id='" + id + "', fields=" + fields + "}";
    }

    public boolean isValid() {
        return id != null && !id.trim().isEmpty();
    }


    public String getValidationErrors() {
        List<String> errors = new ArrayList<>();

        if (id == null || id.trim().isEmpty()) {
            errors.add("id is required and must not be blank");
        }

        for (Map.Entry<String, List<Object>> entry : fields.entrySet()) {
            String name = entry.getKey();
            List<Object> values = entry.getValue();

            if (name == null || name.trim().isEmpty()) {
                errors.add("field name must not be null or blank");
            } else if (values == null) {
                errors.add("field '" + name + "' has a null values list");
            } else {
                for (int i = 0; i < values.size(); i++) {
                    if (values.get(i) == null) {
                        errors.add("field '" + name + "' has null value at index " + i);
                    }
                }
            }
        }

        return String.join("; ", errors);
    }
}
