package io.synapsedb.document;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for FieldConfig
 * @author Amit Tiwari
 */
class FieldConfigTest {

    @Test
    void testDefaultConfig() {
        FieldConfig config = FieldConfig.defaults();

        assertTrue(config.isStored());
        assertTrue(config.isIndexed());
        assertTrue(config.isTokenized());
        assertEquals(FieldConfig.FieldType.TEXT, config.getType());
    }

    @Test
    void testTextConfig() {
        FieldConfig config = FieldConfig.text();

        assertTrue(config.isStored());
        assertTrue(config.isIndexed());
        assertTrue(config.isTokenized());
        assertEquals(FieldConfig.FieldType.TEXT, config.getType());
    }

    @Test
    void testKeywordConfig() {
        FieldConfig config = FieldConfig.keyword();

        assertTrue(config.isStored());
        assertTrue(config.isIndexed());
        assertFalse(config.isTokenized());
        assertEquals(FieldConfig.FieldType.KEYWORD, config.getType());
    }

    @Test
    void testNumericConfig() {
        FieldConfig config = FieldConfig.number(FieldConfig.FieldType.LONG);

        assertTrue(config.isStored());
        assertTrue(config.isIndexed());
        assertFalse(config.isTokenized());
        assertEquals(FieldConfig.FieldType.LONG, config.getType());
    }

    @Test
    void testStoredOnlyConfig() {
        FieldConfig config = FieldConfig.storedOnly();

        assertTrue(config.isStored());
        assertFalse(config.isIndexed());
    }

    @Test
    void testIndexedOnlyConfig() {
        FieldConfig config = FieldConfig.indexedOnly();

        assertFalse(config.isStored());
        assertTrue(config.isIndexed());
    }

    @Test
    void testFluentAPI() {
        FieldConfig config = FieldConfig.defaults()
                .setStored(false)
                .setIndexed(true)
                .setTokenized(false)
                .setType(FieldConfig.FieldType.KEYWORD);

        assertFalse(config.isStored());
        assertTrue(config.isIndexed());
        assertFalse(config.isTokenized());
        assertEquals(FieldConfig.FieldType.KEYWORD, config.getType());
    }

    @Test
    void testAllFieldTypes() {
        FieldConfig.FieldType[] types = FieldConfig.FieldType.values();

        assertTrue(types.length >= 10);

        // Verify specific types exist
        assertNotNull(FieldConfig.FieldType.TEXT);
        assertNotNull(FieldConfig.FieldType.KEYWORD);
        assertNotNull(FieldConfig.FieldType.LONG);
        assertNotNull(FieldConfig.FieldType.INTEGER);
        assertNotNull(FieldConfig.FieldType.DOUBLE);
        assertNotNull(FieldConfig.FieldType.FLOAT);
        assertNotNull(FieldConfig.FieldType.BOOLEAN);
        assertNotNull(FieldConfig.FieldType.DATE);
        assertNotNull(FieldConfig.FieldType.BINARY);
        assertNotNull(FieldConfig.FieldType.OBJECT);
    }

    @Test
    void testConfigImmutability() {
        FieldConfig config1 = FieldConfig.text();
        FieldConfig config2 = config1.setStored(false);

        // Fluent API returns same object (mutable)
        assertSame(config1, config2);
        assertFalse(config1.isStored());
    }
}