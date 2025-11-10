package io.synapsedb.core.index.mapping;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test cases for MappingBuilder.
 *
 * @author Amit Tiwari
 */
class MappingBuilderTest {

    @Test
    void testCreateEmptyMapping() {
        IndexMapping mapping = MappingBuilder.create()
            .build();

        assertNotNull(mapping);
        assertTrue(mapping.fields().isEmpty());
        assertEquals(DynamicMapping.enabled(), mapping.dynamic());
    }

    @Test
    void testAddTextField() {
        IndexMapping mapping = MappingBuilder.create()
            .addTextField("title", true, "standard")
            .build();

        assertTrue(mapping.hasField("title"));
        FieldMapping field = mapping.field("title").orElseThrow();
        assertEquals(FieldMapping.FieldType.TEXT, field.type());
        assertTrue(field.stored());
        assertTrue(field.indexed());
        assertEquals("standard", field.analyzer().orElse(null));
    }

    @Test
    void testAddKeywordField() {
        IndexMapping mapping = MappingBuilder.create()
            .addKeywordField("category", true, true)
            .build();

        assertTrue(mapping.hasField("category"));
        FieldMapping field = mapping.field("category").orElseThrow();
        assertEquals(FieldMapping.FieldType.KEYWORD, field.type());
        assertTrue(field.stored());
        assertTrue(field.indexed());
        assertTrue(field.docValues());
    }

    @Test
    void testAddLongField() {
        IndexMapping mapping = MappingBuilder.create()
            .addLongField("timestamp", true)
            .build();

        assertTrue(mapping.hasField("timestamp"));
        FieldMapping field = mapping.field("timestamp").orElseThrow();
        assertEquals(FieldMapping.FieldType.LONG, field.type());
        assertTrue(field.docValues());
        assertFalse(field.stored());
    }

    @Test
    void testAddDoubleField() {
        IndexMapping mapping = MappingBuilder.create()
            .addDoubleField("price", true, true)
            .build();

        assertTrue(mapping.hasField("price"));
        FieldMapping field = mapping.field("price").orElseThrow();
        assertEquals(FieldMapping.FieldType.DOUBLE, field.type());
        assertTrue(field.stored());
        assertTrue(field.docValues());
    }

    @Test
    void testAddDateField() {
        IndexMapping mapping = MappingBuilder.create()
            .addDateField("created_at", false, true)
            .build();

        assertTrue(mapping.hasField("created_at"));
        FieldMapping field = mapping.field("created_at").orElseThrow();
        assertEquals(FieldMapping.FieldType.DATE, field.type());
        assertFalse(field.stored());
        assertTrue(field.docValues());
    }

    @Test
    void testAddBooleanField() {
        IndexMapping mapping = MappingBuilder.create()
            .addBooleanField("is_active", true)
            .build();

        assertTrue(mapping.hasField("is_active"));
        FieldMapping field = mapping.field("is_active").orElseThrow();
        assertEquals(FieldMapping.FieldType.BOOLEAN, field.type());
        assertTrue(field.docValues());
    }

    @Test
    void testMultipleFields() {
        IndexMapping mapping = MappingBuilder.create()
            .addTextField("title", true, "standard")
            .addKeywordField("category", true)
            .addLongField("timestamp", true)
            .addDoubleField("price", false, true)
            .addBooleanField("is_active", true)
            .build();

        assertEquals(5, mapping.fields().size());
        assertTrue(mapping.hasField("title"));
        assertTrue(mapping.hasField("category"));
        assertTrue(mapping.hasField("timestamp"));
        assertTrue(mapping.hasField("price"));
        assertTrue(mapping.hasField("is_active"));
    }

    @Test
    void testDynamicMappingEnabled() {
        IndexMapping mapping = MappingBuilder.create()
            .addTextField("title")
            .dynamicEnabled()
            .build();

        assertEquals(DynamicMapping.Mode.ENABLED, mapping.dynamic().mode());
        assertTrue(mapping.dynamic().allowUnknownFields());
    }

    @Test
    void testDynamicMappingDisabled() {
        IndexMapping mapping = MappingBuilder.create()
            .addTextField("title")
            .dynamicDisabled()
            .build();

        assertEquals(DynamicMapping.Mode.DISABLED, mapping.dynamic().mode());
        assertFalse(mapping.dynamic().allowUnknownFields());
    }

    @Test
    void testDynamicMappingStrict() {
        IndexMapping mapping = MappingBuilder.create()
            .addTextField("title")
            .dynamicStrict()
            .build();

        assertEquals(DynamicMapping.Mode.STRICT, mapping.dynamic().mode());
        assertTrue(mapping.dynamic().rejectUnknownFields());
    }

    @Test
    void testRemoveField() {
        IndexMapping mapping = MappingBuilder.create()
            .addTextField("title")
            .addKeywordField("category")
            .removeField("category")
            .build();

        assertEquals(1, mapping.fields().size());
        assertTrue(mapping.hasField("title"));
        assertFalse(mapping.hasField("category"));
    }

    @Test
    void testFieldBuilderWithFullControl() {
        IndexMapping mapping = MappingBuilder.create()
            .field("custom_field", FieldMapping.FieldType.TEXT)
                .stored(true)
                .indexed(true)
                .analyzer("english")
                .end()
            .build();

        assertTrue(mapping.hasField("custom_field"));
        FieldMapping field = mapping.field("custom_field").orElseThrow();
        assertEquals(FieldMapping.FieldType.TEXT, field.type());
        assertTrue(field.stored());
        assertTrue(field.indexed());
        assertEquals("english", field.analyzer().orElse(null));
    }

    @Test
    void testFieldBuilderChaining() {
        IndexMapping mapping = MappingBuilder.create()
            .field("field1", FieldMapping.FieldType.TEXT)
                .stored(true)
                .analyzer("standard")
                .field("field2", FieldMapping.FieldType.KEYWORD)
                    .stored(false)
                    .docValues(true)
                    .build();

        assertEquals(2, mapping.fields().size());
        assertTrue(mapping.hasField("field1"));
        assertTrue(mapping.hasField("field2"));
    }

    @Test
    void testFieldBuilderDirectBuild() {
        IndexMapping mapping = MappingBuilder.create()
            .field("only_field", FieldMapping.FieldType.LONG)
                .stored(false)
                .docValues(true)
                .build();

        assertEquals(1, mapping.fields().size());
        FieldMapping field = mapping.field("only_field").orElseThrow();
        assertEquals(FieldMapping.FieldType.LONG, field.type());
        assertFalse(field.stored());
        assertTrue(field.docValues());
    }

    @Test
    void testComplexMappingExample() {
        // Simulate a blog post mapping
        IndexMapping mapping = MappingBuilder.create()
            .addTextField("title", true, "standard")
            .addTextField("content", true, "english")
            .addKeywordField("author", true)
            .addKeywordField("category", true, true)
            .addKeywordField("tags", false, true)
            .addDateField("published_at", false, true)
            .addDateField("updated_at", false, true)
            .addLongField("view_count", true)
            .addDoubleField("rating", true)
            .addBooleanField("is_published", true)
            .dynamicStrict()
            .build();

        assertEquals(10, mapping.fields().size());
        assertEquals(DynamicMapping.Mode.STRICT, mapping.dynamic().mode());

        // Verify text fields
        FieldMapping title = mapping.field("title").orElseThrow();
        assertEquals(FieldMapping.FieldType.TEXT, title.type());
        assertEquals("standard", title.analyzer().orElse(null));

        FieldMapping content = mapping.field("content").orElseThrow();
        assertEquals(FieldMapping.FieldType.TEXT, content.type());
        assertEquals("english", content.analyzer().orElse(null));

        // Verify keyword fields
        assertTrue(mapping.field("author").isPresent());
        assertTrue(mapping.field("category").isPresent());
        assertTrue(mapping.field("tags").isPresent());

        // Verify date fields
        FieldMapping publishedAt = mapping.field("published_at").orElseThrow();
        assertEquals(FieldMapping.FieldType.DATE, publishedAt.type());
        assertTrue(publishedAt.docValues());

        // Verify numeric fields
        FieldMapping viewCount = mapping.field("view_count").orElseThrow();
        assertEquals(FieldMapping.FieldType.LONG, viewCount.type());

        FieldMapping rating = mapping.field("rating").orElseThrow();
        assertEquals(FieldMapping.FieldType.DOUBLE, rating.type());

        // Verify boolean field
        FieldMapping isPublished = mapping.field("is_published").orElseThrow();
        assertEquals(FieldMapping.FieldType.BOOLEAN, isPublished.type());
    }

    @Test
    void testDefaultOverloads() {
        // Test convenience methods with defaults
        IndexMapping mapping = MappingBuilder.create()
            .addTextField("title")  // stored=true, no analyzer
            .addKeywordField("category")  // stored=true, docValues=true
            .addLongField("timestamp")  // stored=false, docValues=true
            .addDoubleField("price")  // stored=false, docValues=true
            .addDateField("created_at")  // stored=false, docValues=true
            .addBooleanField("is_active")  // stored=false, docValues=true
            .build();

        assertEquals(6, mapping.fields().size());

        // Verify defaults
        assertTrue(mapping.field("title").orElseThrow().stored());
        assertTrue(mapping.field("category").orElseThrow().stored());
        assertFalse(mapping.field("timestamp").orElseThrow().stored());
        assertFalse(mapping.field("price").orElseThrow().stored());
        assertFalse(mapping.field("created_at").orElseThrow().stored());
        assertFalse(mapping.field("is_active").orElseThrow().stored());

        // All should have appropriate indexing/docValues
        assertTrue(mapping.field("title").orElseThrow().indexed());
        assertTrue(mapping.field("category").orElseThrow().docValues());
        assertTrue(mapping.field("timestamp").orElseThrow().docValues());
    }
}

