package io.synapsedb.document;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for Document
 * @author Amit Tiwari
 */
class DocumentTest {

    private Document document;

    @BeforeEach
    void setUp() {
        document = new Document("test-doc-1");
    }

    // ============ Constructor Tests ============

    @Test
    void testConstructorWithId() {
        Document doc = new Document("doc123");
        assertEquals("doc123", doc.getId());
        assertTrue(doc.isValid());
    }

    @Test
    void testConstructorWithoutId() {
        Document doc = new Document();
        assertNull(doc.getId());
        assertFalse(doc.isValid());
    }

    @Test
    void testSetId() {
        Document doc = new Document();
        doc.setId("new-id");
        assertEquals("new-id", doc.getId());
        assertTrue(doc.isValid());
    }

    // ============ Single Value Field Tests ============

    @Test
    void testAddSingleField() {
        document.addField("title", "Test Title");

        assertTrue(document.hasField("title"));
        assertEquals("Test Title", document.getField("title"));
    }

    @Test
    void testAddFieldWithConfig() {
        FieldConfig config = FieldConfig.keyword();
        document.addField("category", "Technology", config);

        assertTrue(document.hasField("category"));
        assertEquals("Technology", document.getField("category"));
        assertEquals(config, document.getFieldConfig("category"));
    }

    @Test
    void testAddMultipleFieldsWithSameName() {
        document.addField("tag", "java");
        document.addField("tag", "lucene");
        document.addField("tag", "search");

        List<Object> tags = document.getFields("tag");
        assertEquals(3, tags.size());
        assertTrue(tags.contains("java"));
        assertTrue(tags.contains("lucene"));
        assertTrue(tags.contains("search"));
    }

    @Test
    void testGetFieldReturnsFirstValue() {
        document.addField("tag", "first");
        document.addField("tag", "second");

        assertEquals("first", document.getField("tag"));
    }

    // ============ Multi-Value Field Tests ============

    @Test
    void testAddFieldsWithList() {
        List<Object> authors = Arrays.asList("Alice", "Bob", "Charlie");
        document.addFields("authors", authors);

        List<Object> retrieved = document.getFields("authors");
        assertEquals(3, retrieved.size());
        assertEquals(authors, retrieved);
    }

    @Test
    void testAddFieldsWithConfig() {
        List<Object> keywords = Arrays.asList("java", "lucene", "search");
        FieldConfig config = FieldConfig.keyword();

        document.addFields("keywords", keywords, config);

        assertEquals(3, document.getFields("keywords").size());
        assertEquals(config, document.getFieldConfig("keywords"));
    }

    @Test
    void testAddFieldsToExistingField() {
        document.addField("tag", "existing");

        List<Object> newTags = Arrays.asList("new1", "new2");
        document.addFields("tag", newTags);

        List<Object> allTags = document.getFields("tag");
        assertEquals(3, allTags.size());
        assertTrue(allTags.contains("existing"));
        assertTrue(allTags.contains("new1"));
        assertTrue(allTags.contains("new2"));
    }

    // ============ Getter Tests ============

    @Test
    void testGetFieldReturnsNullForNonExistentField() {
        assertNull(document.getField("nonexistent"));
    }

    @Test
    void testGetFieldsReturnsEmptyListForNonExistentField() {
        List<Object> result = document.getFields("nonexistent");
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetFieldNames() {
        document.addField("title", "Test");
        document.addField("content", "Content");
        document.addField("author", "Alice");

        Set<String> fieldNames = document.getFieldNames();
        assertEquals(3, fieldNames.size());
        assertTrue(fieldNames.contains("title"));
        assertTrue(fieldNames.contains("content"));
        assertTrue(fieldNames.contains("author"));
    }

    @Test
    void testGetAllFields() {
        document.addField("title", "Test");
        document.addField("tag", "java");
        document.addField("tag", "lucene");

        Map<String, List<Object>> allFields = document.getAllFields();
        assertEquals(2, allFields.size());
        assertEquals(1, allFields.get("title").size());
        assertEquals(2, allFields.get("tag").size());
    }

    @Test
    void testGetFieldConfig() {
        FieldConfig config = FieldConfig.keyword();
        document.addField("id", "123", config);

        assertEquals(config, document.getFieldConfig("id"));
    }

    @Test
    void testGetFieldConfigReturnsDefaultForUnknownField() {
        FieldConfig config = document.getFieldConfig("nonexistent");
        assertNotNull(config);
        // Should return default config
    }

    @Test
    void testHasField() {
        document.addField("title", "Test");

        assertTrue(document.hasField("title"));
        assertFalse(document.hasField("nonexistent"));
    }

    @Test
    void testGetFieldCount() {
        assertEquals(0, document.getFieldCount());

        document.addField("field1", "value1");
        assertEquals(1, document.getFieldCount());

        document.addField("field2", "value2");
        assertEquals(2, document.getFieldCount());

        // Adding to same field doesn't increase count
        document.addField("field1", "value3");
        assertEquals(2, document.getFieldCount());
    }

    // ============ Validation Tests ============

    @Test
    void testIsValidWithId() {
        Document doc = new Document("valid-id");
        assertTrue(doc.isValid());
    }

    @Test
    void testIsValidWithoutId() {
        Document doc = new Document();
        assertFalse(doc.isValid());
    }

    @Test
    void testIsValidWithEmptyId() {
        Document doc = new Document("");
        assertFalse(doc.isValid());
    }



    @Test
    void testGetValidationErrorsForMissingId() {
        Document doc = new Document();
        String errors = doc.getValidationErrors();

        assertNotNull(errors);
        assertTrue(errors.contains("id is required"));
    }

    @Test
    void testGetValidationErrorsForEmptyId() {
        Document doc = new Document("");
        String errors = doc.getValidationErrors();

        assertTrue(errors.contains("id is required"));
    }

    @Test
    void testGetValidationErrorsForNullFieldValue() {
        document.addField("field1", null);

        String errors = document.getValidationErrors();
        assertTrue(errors.contains("null value"));
    }

    @Test
    void testGetValidationErrorsForValidDocument() {
        document.addField("title", "Test");

        String errors = document.getValidationErrors();
        assertTrue(errors.isEmpty() || errors.isBlank());
    }

    // ============ Edge Cases ============

    @Test
    void testAddFieldWithNullValue() {
        document.addField("nullField", null);

        assertTrue(document.hasField("nullField"));
        assertNull(document.getField("nullField"));
    }

    @Test
    void testAddFieldWithEmptyString() {
        document.addField("emptyField", "");

        assertTrue(document.hasField("emptyField"));
        assertEquals("", document.getField("emptyField"));
    }

    @Test
    void testAddFieldsWithEmptyList() {
        document.addFields("emptyList", Collections.emptyList());

        assertTrue(document.hasField("emptyList"));
        assertTrue(document.getFields("emptyList").isEmpty());
    }

    @Test
    void testAddFieldsWithNullList() {
        assertThrows(NullPointerException.class, () -> {
            document.addFields("nullList", null);
        });
    }

    @Test
    void testFieldOrderPreserved() {
        document.addField("field1", "value1");
        document.addField("field2", "value2");
        document.addField("field3", "value3");

        List<String> fieldNames = new ArrayList<>(document.getFieldNames());
        assertEquals("field1", fieldNames.get(0));
        assertEquals("field2", fieldNames.get(1));
        assertEquals("field3", fieldNames.get(2));
    }

    // ============ Complex Types Tests ============

    @Test
    void testAddNumericFields() {
        document.addField("integer", 42, FieldConfig.number(FieldConfig.FieldType.INTEGER));
        document.addField("long", 123456789L, FieldConfig.number(FieldConfig.FieldType.LONG));
        document.addField("double", 3.14159, FieldConfig.number(FieldConfig.FieldType.DOUBLE));
        document.addField("float", 2.71828f, FieldConfig.number(FieldConfig.FieldType.FLOAT));

        assertEquals(42, document.getField("integer"));
        assertEquals(123456789L, document.getField("long"));
        assertEquals(3.14159, document.getField("double"));
        assertEquals(2.71828f, document.getField("float"));
    }

    @Test
    void testAddBooleanField() {
        document.addField("active", true, FieldConfig.defaults().setType(FieldConfig.FieldType.BOOLEAN));

        assertEquals(true, document.getField("active"));
    }

    @Test
    void testAddDateField() {
        long timestamp = System.currentTimeMillis();
        document.addField("created", timestamp, FieldConfig.defaults().setType(FieldConfig.FieldType.DATE));

        assertEquals(timestamp, document.getField("created"));
    }

    @Test
    void testAddBinaryField() {
        byte[] data = {1, 2, 3, 4, 5};
        document.addField("data", data, FieldConfig.defaults().setType(FieldConfig.FieldType.BINARY));

        assertArrayEquals(data, (byte[]) document.getField("data"));
    }

    // ============ Fluent API Tests ============

    @Test
    void testFluentAPI() {
        Document doc = new Document("doc1")
                .addField("title", "Test")
                .addField("content", "Content")
                .addField("author", "Alice");

        assertEquals(3, doc.getFieldCount());
        assertEquals("Test", doc.getField("title"));
        assertEquals("Content", doc.getField("content"));
        assertEquals("Alice", doc.getField("author"));
    }

    @Test
    void testFluentAPIWithMultipleValues() {
        Document doc = new Document("doc1")
                .addField("tag", "java")
                .addField("tag", "lucene")
                .addFields("authors", Arrays.asList("Alice", "Bob"));

        assertEquals(2, doc.getFieldCount());
        assertEquals(2, doc.getFields("tag").size());
        assertEquals(2, doc.getFields("authors").size());
    }

    // ============ ToString Test ============

    @Test
    void testToString() {
        document.addField("title", "Test");
        document.addField("count", 42);

        String str = document.toString();

        assertNotNull(str);
        assertTrue(str.contains("test-doc-1"));
        assertTrue(str.contains("title"));
    }

    @Test
    void testToStringWithEmptyDocument() {
        String str = document.toString();

        assertNotNull(str);
        assertTrue(str.contains("test-doc-1"));
    }
}