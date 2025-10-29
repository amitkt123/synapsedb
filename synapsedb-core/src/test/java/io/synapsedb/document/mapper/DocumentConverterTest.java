package io.synapsedb.document.mapper;

import io.synapsedb.document.Document;
import io.synapsedb.document.FieldConfig;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexableField;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for DocumentConverter
 */
class DocumentConverterTest {

    private Document synapseDoc;

    @BeforeEach
    void setUp() {
        synapseDoc = new Document("test-doc-1");
    }

    // ============ Null/Empty Tests ============

    @Test
    void testToLuceneDocumentWithNull() {
        assertThrows(IllegalArgumentException.class, () -> {
            DocumentConverter.toLuceneDocument(null);
        });
    }

    @Test
    void testToLuceneDocumentWithoutId() {
        Document doc = new Document();

        assertThrows(IllegalArgumentException.class, () -> {
            DocumentConverter.toLuceneDocument(doc);
        });
    }

    @Test
    void testFromLuceneDocumentWithNull() {
        assertThrows(IllegalArgumentException.class, () -> {
            DocumentConverter.fromLuceneDocument(null);
        });
    }

    @Test
    void testFromLuceneDocumentWithoutIdField() {
        org.apache.lucene.document.Document luceneDoc = new org.apache.lucene.document.Document();
        luceneDoc.add(new TextField("title", "Test", Field.Store.YES));

        assertThrows(IllegalArgumentException.class, () -> {
            DocumentConverter.fromLuceneDocument(luceneDoc);
        });
    }

    // ============ Basic Conversion Tests ============

    @Test
    void testConvertEmptyDocument() {
        org.apache.lucene.document.Document luceneDoc =
                DocumentConverter.toLuceneDocument(synapseDoc);

        assertNotNull(luceneDoc);
        assertEquals("test-doc-1", luceneDoc.get("_id"));
    }

    @Test
    void testConvertDocumentWithTextField() {
        synapseDoc.addField("title", "Test Title", FieldConfig.text());

        org.apache.lucene.document.Document luceneDoc =
                DocumentConverter.toLuceneDocument(synapseDoc);

        assertNotNull(luceneDoc);
        assertEquals("test-doc-1", luceneDoc.get("_id"));
        assertEquals("Test Title", luceneDoc.get("title"));

        // Verify field type
        IndexableField field = luceneDoc.getField("title");
        assertTrue(field instanceof TextField);
    }

    @Test
    void testConvertDocumentWithKeywordField() {
        synapseDoc.addField("category", "Technology", FieldConfig.keyword());

        org.apache.lucene.document.Document luceneDoc =
                DocumentConverter.toLuceneDocument(synapseDoc);

        assertEquals("Technology", luceneDoc.get("category"));

        IndexableField field = luceneDoc.getField("category");
        assertTrue(field instanceof StringField);
    }

    // ============ Numeric Field Tests ============

    @Test
    void testConvertLongField() {
        synapseDoc.addField("count", 12345L, FieldConfig.number(FieldConfig.FieldType.LONG));

        org.apache.lucene.document.Document luceneDoc =
                DocumentConverter.toLuceneDocument(synapseDoc);

        // Should have both LongPoint (for searching) and StoredField (for retrieval)
        IndexableField[] fields = luceneDoc.getFields("count");
        assertTrue(fields.length >= 1);

        // Check stored value
        IndexableField storedField = luceneDoc.getField("count");
        assertNotNull(storedField);
    }

    @Test
    void testConvertIntegerField() {
        synapseDoc.addField("age", 30, FieldConfig.number(FieldConfig.FieldType.INTEGER));

        org.apache.lucene.document.Document luceneDoc =
                DocumentConverter.toLuceneDocument(synapseDoc);

        IndexableField[] fields = luceneDoc.getFields("age");
        assertTrue(fields.length >= 1);
    }

    @Test
    void testConvertDoubleField() {
        synapseDoc.addField("price", 99.99, FieldConfig.number(FieldConfig.FieldType.DOUBLE));

        org.apache.lucene.document.Document luceneDoc =
                DocumentConverter.toLuceneDocument(synapseDoc);

        IndexableField[] fields = luceneDoc.getFields("price");
        assertTrue(fields.length >= 1);
    }

    @Test
    void testConvertFloatField() {
        synapseDoc.addField("rating", 4.5f, FieldConfig.number(FieldConfig.FieldType.FLOAT));

        org.apache.lucene.document.Document luceneDoc =
                DocumentConverter.toLuceneDocument(synapseDoc);

        IndexableField[] fields = luceneDoc.getFields("rating");
        assertTrue(fields.length >= 1);
    }

    // ============ Stored/Indexed Configuration Tests ============

    @Test
    void testStoredAndIndexedField() {
        FieldConfig config = FieldConfig.text()
                .setStored(true)
                .setIndexed(true);

        synapseDoc.addField("content", "Searchable content", config);

        org.apache.lucene.document.Document luceneDoc =
                DocumentConverter.toLuceneDocument(synapseDoc);

        assertEquals("Searchable content", luceneDoc.get("content"));
    }

    @Test
    void testIndexedOnlyField() {
        FieldConfig config = FieldConfig.indexedOnly();

        synapseDoc.addField("searchable", "Search but don't store", config);

        org.apache.lucene.document.Document luceneDoc =
                DocumentConverter.toLuceneDocument(synapseDoc);

        // Should be indexable but value might not be stored
        IndexableField field = luceneDoc.getField("searchable");
        assertNotNull(field);
    }


    @Test
    void testConvertMultiValuedField() {
        synapseDoc.addField("tag", "java");
        synapseDoc.addField("tag", "lucene");
        synapseDoc.addField("tag", "search");

        org.apache.lucene.document.Document luceneDoc =
                DocumentConverter.toLuceneDocument(synapseDoc);

        String[] tags = luceneDoc.getValues("tag");
        assertEquals(3, tags.length);
        assertTrue(Arrays.asList(tags).contains("java"));
        assertTrue(Arrays.asList(tags).contains("lucene"));
        assertTrue(Arrays.asList(tags).contains("search"));
    }

    // ============ Round-Trip Conversion Tests ============

    @Test
    void testRoundTripWithTextField() {
        synapseDoc.addField("title", "Original Title", FieldConfig.text());

        org.apache.lucene.document.Document luceneDoc =
                DocumentConverter.toLuceneDocument(synapseDoc);
        Document restored =
                DocumentConverter.fromLuceneDocument(luceneDoc);

        assertEquals(synapseDoc.getId(), restored.getId());
        assertEquals("Original Title", restored.getField("title"));
    }

    @Test
    void testRoundTripWithKeywordField() {
        synapseDoc.addField("category", "Tech", FieldConfig.keyword());

        org.apache.lucene.document.Document luceneDoc =
                DocumentConverter.toLuceneDocument(synapseDoc);
        Document restored =
                DocumentConverter.fromLuceneDocument(luceneDoc);

        assertEquals("Tech", restored.getField("category"));
    }

    @Test
    void testRoundTripWithNumericField() {
        synapseDoc.addField("count", 42L, FieldConfig.number(FieldConfig.FieldType.LONG));

        org.apache.lucene.document.Document luceneDoc =
                DocumentConverter.toLuceneDocument(synapseDoc);
        Document restored =
                DocumentConverter.fromLuceneDocument(luceneDoc);

        Object value = restored.getField("count");
        assertNotNull(value);
        assertTrue(value instanceof Number);
        assertEquals(42L, ((Number) value).longValue());
    }

    @Test
    void testRoundTripWithMultipleFields() {
        synapseDoc.addField("title", "Test", FieldConfig.text());
        synapseDoc.addField("category", "Tech", FieldConfig.keyword());
        synapseDoc.addField("count", 100, FieldConfig.number(FieldConfig.FieldType.INTEGER));

        org.apache.lucene.document.Document luceneDoc =
                DocumentConverter.toLuceneDocument(synapseDoc);
        Document restored =
                DocumentConverter.fromLuceneDocument(luceneDoc);

        assertEquals(synapseDoc.getId(), restored.getId());
        assertEquals(3, restored.getFieldCount());
        assertEquals("Test", restored.getField("title"));
        assertEquals("Tech", restored.getField("category"));
        assertNotNull(restored.getField("count"));
    }

    @Test
    void testRoundTripWithMultiValuedField() {
        synapseDoc.addField("tag", "java");
        synapseDoc.addField("tag", "lucene");
        synapseDoc.addField("tag", "search");

        org.apache.lucene.document.Document luceneDoc =
                DocumentConverter.toLuceneDocument(synapseDoc);
        Document restored =
                DocumentConverter.fromLuceneDocument(luceneDoc);

        assertEquals(3, restored.getFields("tag").size());
        assertTrue(restored.getFields("tag").contains("java"));
        assertTrue(restored.getFields("tag").contains("lucene"));
        assertTrue(restored.getFields("tag").contains("search"));
    }

    // ============ Deduplication Tests ============

    @Test
    void testDeduplicationInRoundTrip() {
        // This tests the deduplication logic in fromLuceneDocument
        org.apache.lucene.document.Document luceneDoc = new org.apache.lucene.document.Document();
        luceneDoc.add(new StringField("_id", "test-1", Field.Store.YES));

        // Add duplicate field (simulating Point + StoredField scenario)
        luceneDoc.add(new StringField("category", "Tech", Field.Store.YES));
        luceneDoc.add(new StoredField("category", "Tech"));  // Duplicate

        Document restored = DocumentConverter.fromLuceneDocument(luceneDoc);

        // Should deduplicate to single value
        assertEquals(1, restored.getFields("category").size());
        assertEquals("Tech", restored.getField("category"));
    }

    // ============ Special Field Tests ============

    @Test
    void testBooleanField() {
        synapseDoc.addField("active", true,
                FieldConfig.defaults().setType(FieldConfig.FieldType.BOOLEAN));

        org.apache.lucene.document.Document luceneDoc =
                DocumentConverter.toLuceneDocument(synapseDoc);

        String value = luceneDoc.get("active");
        assertNotNull(value);
        assertTrue(value.equals("true") || value.equals("True"));
    }

    @Test
    void testDateField() {
        long timestamp = System.currentTimeMillis();
        synapseDoc.addField("created", timestamp,
                FieldConfig.defaults().setType(FieldConfig.FieldType.DATE));

        org.apache.lucene.document.Document luceneDoc =
                DocumentConverter.toLuceneDocument(synapseDoc);

        IndexableField[] fields = luceneDoc.getFields("created");
        assertTrue(fields.length >= 1);
    }

    @Test
    void testBinaryField() {
        byte[] data = {1, 2, 3, 4, 5};
        synapseDoc.addField("data", data,
                FieldConfig.defaults().setType(FieldConfig.FieldType.BINARY));

        org.apache.lucene.document.Document luceneDoc =
                DocumentConverter.toLuceneDocument(synapseDoc);

        IndexableField field = luceneDoc.getField("data");
        assertNotNull(field);
    }

    // ============ Edge Cases ============

    @Test
    void testConvertWithNullFieldValue() {
        synapseDoc.addField("nullField", null);

        org.apache.lucene.document.Document luceneDoc =
                DocumentConverter.toLuceneDocument(synapseDoc);

        // Null values should be skipped
        assertNull(luceneDoc.get("nullField"));
    }


@Test
void testConvertComplexDocument() {
    synapseDoc
            .addField("title", "Complex Document", FieldConfig.text())
            .addField("category", "Technology", FieldConfig.keyword())
            .addField("price", 99.99, FieldConfig.number(FieldConfig.FieldType.DOUBLE))
            .addField("quantity", 10, FieldConfig.number(FieldConfig.FieldType.INTEGER))
            .addField("active", true, FieldConfig.defaults().setType(FieldConfig.FieldType.BOOLEAN))
            .addField("created", System.currentTimeMillis(), FieldConfig.defaults().setType(FieldConfig.FieldType.DATE));

    synapseDoc.addField("tag", "java");
    synapseDoc.addField("tag", "lucene");

    org.apache.lucene.document.Document luceneDoc =
            DocumentConverter.toLuceneDocument(synapseDoc);

    assertNotNull(luceneDoc);
    assertEquals("test-doc-1", luceneDoc.get("_id"));
    assertEquals("Complex Document", luceneDoc.get("title"));
    assertEquals("Technology", luceneDoc.get("category"));
    assertEquals(2, luceneDoc.getValues("tag").length);
}

@Test
void testRoundTripComplexDocument() {
    synapseDoc
            .addField("title", "Complex Document", FieldConfig.text())
            .addField("category", "Technology", FieldConfig.keyword())
            .addField("count", 100L, FieldConfig.number(FieldConfig.FieldType.LONG));

    synapseDoc.addField("tag", "java");
    synapseDoc.addField("tag", "lucene");

    org.apache.lucene.document.Document luceneDoc =
            DocumentConverter.toLuceneDocument(synapseDoc);
    Document restored =
            DocumentConverter.fromLuceneDocument(luceneDoc);

    assertEquals(synapseDoc.getId(), restored.getId());
    assertEquals("Complex Document", restored.getField("title"));
    assertEquals("Technology", restored.getField("category"));
    assertEquals(2, restored.getFields("tag").size());
    assertNotNull(restored.getField("count"));
}

// ============ Internal Field Tests ============

@Test
void testInternalFieldsNotExposed() {
    synapseDoc.addField("title", "Test");

    org.apache.lucene.document.Document luceneDoc =
            DocumentConverter.toLuceneDocument(synapseDoc);

    // Add some internal Lucene fields
    luceneDoc.add(new StoredField("_internal", "secret"));

    Document restored = DocumentConverter.fromLuceneDocument(luceneDoc);

    // Internal fields starting with _ should be filtered out
    assertFalse(restored.hasField("_internal"));
}

@Test
void testIdFieldIsPreserved() {
    synapseDoc.addField("title", "Test");

    org.apache.lucene.document.Document luceneDoc =
            DocumentConverter.toLuceneDocument(synapseDoc);
    Document restored =
            DocumentConverter.fromLuceneDocument(luceneDoc);

    assertEquals("test-doc-1", restored.getId());
}
}