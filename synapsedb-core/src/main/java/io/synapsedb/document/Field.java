package io.synapsedb.document;

import java.util.Objects;

/**
 * Represents a single field in a document.
 * A field has a name, value, and type.
 */
public class Field {
    private final String name;
    private final Object value;
    private final FieldType fieldType;
    private final boolean stored;
    private final boolean indexed;

    public Field(String name, Object value, FieldType fieldType, boolean stored, boolean indexed) {
        this.name = Objects.requireNonNull(name);
        this.value = value;
        this.fieldType = Objects.requireNonNull(fieldType);
        this.stored = stored;
        this.indexed = indexed;
    }

    public Field(String name, Object value, FieldType fieldType){
        this(name, value, fieldType, true, true);
    }
    public String getName() {
        return name;
    }

    public Object getValue() {
        return value;
    }

    public FieldType getFieldType() {
        return fieldType;
    }

    public boolean isStored() {
        return stored;
    }

    public boolean isIndexed() {
        return indexed;
    }

    @Override
    public String toString() {
        return "Field{" +
                "name='" + name + '\'' +
                ", value=" + value +
                ", fieldType=" + fieldType +
                ", stored=" + stored +
                ", indexed=" + indexed +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        Field field = (Field) o;
        return stored == field.stored && indexed == field.indexed && Objects.equals(name, field.name) && Objects.equals(value, field.value) && fieldType == field.fieldType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value, fieldType, stored, indexed);
    }
}
