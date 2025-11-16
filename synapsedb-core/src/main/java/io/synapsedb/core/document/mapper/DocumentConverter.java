package io.synapsedb.core.document.mapper;

import io.synapsedb.core.common.helpers.TypeConversionHelpers;
import io.synapsedb.core.document.FieldConfig;
import io.synapsedb.core.document.FieldType;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;


import java.util.*;

/**
 * Converts between SynapseDoc and Lucene Document
 * Handles proper storage and retrieval without duplication
 * @author Amit Tiwari
 */
public class DocumentConverter {

    private DocumentConverter() {
        // Prevent instantiation
    }
    private static final String ID_FIELD = "_id";

    /**
     * Convert SynapseDoc to Lucene Document
     */
    public static org.apache.lucene.document.Document toLuceneDocument(io.synapsedb.core.document.Document synapseDoc) {
        if (synapseDoc == null) {
            throw new IllegalArgumentException("SynapseDoc cannot be null");
        }

        if (synapseDoc.getId() == null || synapseDoc.getId().isEmpty()) {
            throw new IllegalArgumentException("SynapseDoc must have an ID");
        }

        org.apache.lucene.document.Document luceneDoc = new org.apache.lucene.document.Document();


        luceneDoc.add(new StringField(ID_FIELD, synapseDoc.getId(), Field.Store.YES));

        // Convert all fields
        for (String fieldName : synapseDoc.getFieldNames()) {
            List<Object> values = synapseDoc.getFields(fieldName);
            FieldConfig config = synapseDoc.getFieldConfig(fieldName);

            for (Object value : values) {
                addLuceneField(luceneDoc, fieldName, value, config);
            }
        }

        return luceneDoc;
    }

    /**
     * Convert Lucene Document to SynapseDoc
     * ⚠️ CRITICAL: Deduplicate multi-field values
     */
    public static io.synapsedb.core.document.Document fromLuceneDocument(org.apache.lucene.document.Document luceneDoc) {
        if (luceneDoc == null) {
            throw new IllegalArgumentException("Lucene Document cannot be null");
        }

        String id = luceneDoc.get(ID_FIELD);
        if (id == null || id.isEmpty()) {
            throw new IllegalArgumentException("Lucene Document must have " + ID_FIELD + " field");
        }

        io.synapsedb.core.document.Document synapseDoc = new io.synapsedb.core.document.Document(id);

        Map<String, List<Object>> fieldGroups = new LinkedHashMap<>();
        Map<String, FieldConfig> configMap = new HashMap<>();

        for (IndexableField field : luceneDoc.getFields()) {
            String fieldName = field.name();

            // Skip internal Lucene fields
            if (ID_FIELD.equals(fieldName) || fieldName.startsWith("_")) {
                continue;
            }

            // Extract the actual stored value (not the point/docvalue duplicate)
            Object value = extractStoredFieldValue(field);

            if (value != null) {
                // Group by field name
                fieldGroups.computeIfAbsent(fieldName, k -> new ArrayList<>())
                        .add(value);

                // Keep config from first occurrence
                if (!configMap.containsKey(fieldName)) {
                    configMap.put(fieldName, inferFieldConfig(field));
                }
            }
        }

        for (Map.Entry<String, List<Object>> entry : fieldGroups.entrySet()) {
            String fieldName = entry.getKey();
            List<Object> values = entry.getValue();
            FieldConfig config = configMap.get(fieldName);

            // Remove duplicates while preserving order
            Set<String> seen = new LinkedHashSet<>();
            List<Object> deduped = new ArrayList<>();

            for (Object val : values) {
                String strVal = val.toString();
                if (seen.add(strVal)) {
                    deduped.add(val);
                }
            }

            // Add to SynapseDoc
            if (deduped.size() == 1) {
                synapseDoc.addField(fieldName, deduped.get(0), config);
            } else {
                synapseDoc.addFields(fieldName, deduped, config);
            }
        }

        return synapseDoc;
    }

    /**
     * Extract STORED field value only (skip Point/DocValue duplicates)
     */
    private static Object extractStoredFieldValue(IndexableField field) {
        // Only process StoredField and StringField (which are stored)
        if (field instanceof StoredField) {
            return extractValue((StoredField) field);
        }

        if (field instanceof StringField) {
            String value = field.stringValue();
            if (value != null) {
                return value;
            }
        }

        if (field instanceof TextField) {
            return field.stringValue();
        }
        return null;
    }

    /**
     * Extract value from StoredField
     */
    private static Object extractValue(StoredField field) {
        // Try numeric
        Number numValue = field.numericValue();
        if (numValue != null) {
            return numValue;
        }

        // Try string
        String stringValue = field.stringValue();
        if (stringValue != null) {
            return stringValue;
        }

        // Try binary
        BytesRef binaryValue = field.binaryValue();
        if (binaryValue != null) {
            return binaryValue.bytes;
        }

        return null;
    }

    /**
     * Add a field to Lucene document
     *
     */
    private static void addLuceneField(Document doc, String name, Object value,
                                       FieldConfig config) {
        if (value == null) {
            return;
        }

        if (config.getType() == null) {
            throw new IllegalArgumentException("FieldConfig type cannot be null for field: " + name);
        }

        Field.Store store = config.isStored() ? Field.Store.YES : Field.Store.NO;

        if (!config.isIndexed()) {
            // Only store, don't index - use StoredField only
            addStoredOnlyField(doc, name, value);
            return;
        }

        switch (config.getType()) {

            case KEYWORD:
                doc.add(new StringField(name, value.toString(), store));
                break;

            case LONG:
                long longValue = TypeConversionHelpers.toLong(value);
                // Add EITHER Point OR StoredField, not both
                if (config.isStored()) {
                    doc.add(new StoredField(name, longValue));
                    doc.add(new LongPoint(name, longValue));  // For range queries
                } else {
                    doc.add(new LongPoint(name, longValue));  // For queries only
                }
                break;

            case INTEGER:
                int intValue = TypeConversionHelpers.toInt(value);
                if (config.isStored()) {
                    doc.add(new StoredField(name, intValue));
                    doc.add(new IntPoint(name, intValue));
                } else {
                    doc.add(new IntPoint(name, intValue));
                }
                break;

            case DOUBLE:
                double doubleValue = TypeConversionHelpers.toDouble(value);
                if (config.isStored()) {
                    doc.add(new StoredField(name, doubleValue));
                    doc.add(new DoublePoint(name, doubleValue));
                } else {
                    doc.add(new DoublePoint(name, doubleValue));
                }
                break;

            case FLOAT:
                float floatValue = TypeConversionHelpers.toFloat(value);
                if (config.isStored()) {
                    doc.add(new StoredField(name, floatValue));
                    doc.add(new FloatPoint(name, floatValue));
                } else {
                    doc.add(new FloatPoint(name, floatValue));
                }
                break;

            case BOOLEAN:
                String boolStr = value.toString();
                if (config.isStored()) {
                    doc.add(new StoredField(name, boolStr));
                    doc.add(new StringField(name, boolStr, Field.Store.NO));
                } else {
                    doc.add(new StringField(name, boolStr, Field.Store.NO));
                }
                break;

            case DATE:
                long timestamp = TypeConversionHelpers.toTimestamp(value);
                if (config.isStored()) {
                    doc.add(new StoredField(name, timestamp));
                    doc.add(new LongPoint(name, timestamp));
                } else {
                    doc.add(new LongPoint(name, timestamp));
                }
                break;

            case BINARY:
                byte[] bytes = TypeConversionHelpers.toBytes(value);
                doc.add(new StoredField(name, bytes));
                break;

            default:
                doc.add(new TextField(name, value.toString(), store));
        }
    }

    /**
     * Add stored-only field
     */
    private static void addStoredOnlyField(Document doc, String name, Object value) {
        if (value instanceof Long || value instanceof Integer) {
            doc.add(new StoredField(name, ((Number) value).longValue()));
        } else if (value instanceof Double) {
            doc.add(new StoredField(name, (Double) value));
        } else if (value instanceof Float) {
            doc.add(new StoredField(name, (Float) value));
        } else if (value instanceof byte[]) {
            doc.add(new StoredField(name, (byte[]) value));
        } else {
            doc.add(new StoredField(name, value.toString()));
        }
    }

    /**
     * Infer field config from Lucene field
     */
    private static FieldConfig inferFieldConfig(IndexableField field) {
        if (field instanceof TextField) {
            return FieldConfig.builder()
                    .type(FieldType.TEXT)
                    .tokenized(true)
                    .build();
        } else if (field instanceof StringField) {
            return FieldConfig.builder()
                    .type(FieldType.KEYWORD)
                    .tokenized(false)
                    .build();
        } else if (field instanceof IntPoint) {
            return FieldConfig.builder()
                    .type(FieldType.INTEGER)
                    .tokenized(false)
                    .build();
        } else if (field instanceof LongPoint) {
            return FieldConfig.builder()
                    .type(FieldType.LONG)
                    .tokenized(false)
                    .build();
        } else if (field instanceof DoublePoint) {
            return FieldConfig.builder()
                    .type(FieldType.DOUBLE)
                    .tokenized(false)
                    .build();
        } else if (field instanceof FloatPoint) {
            return FieldConfig.builder()
                    .type(FieldType.FLOAT)
                    .tokenized(false)
                    .build();
        } else if (field instanceof StoredField) {
            return FieldConfig.builder()
                    .stored(true)
                    .indexed(false)
                    .tokenized(false)
                    .build();
        }

        // Default fallback
        return FieldConfig.builder().build();
    }

}

