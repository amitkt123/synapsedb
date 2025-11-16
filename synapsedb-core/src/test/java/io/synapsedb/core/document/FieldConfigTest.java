// java
package io.synapsedb.core.document;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for FieldConfig (refactored to use builder and top-level FieldType)
 */
class FieldConfigTest {

    @Test
    void testDefaultConfig() {
        FieldConfig config = FieldConfig.builder().build();

        assertTrue(config.isStored());
        assertTrue(config.isIndexed());
        assertTrue(config.isTokenized());
        assertEquals(FieldType.TEXT, config.getType());
    }

    @Test
    void testTextConfig() {
        FieldConfig config = FieldConfig.builder()
                .type(FieldType.TEXT)
                .tokenized(true)
                .build();

        assertTrue(config.isStored());
        assertTrue(config.isIndexed());
        assertTrue(config.isTokenized());
        assertEquals(FieldType.TEXT, config.getType());
    }

    @Test
    void testKeywordConfig() {
        FieldConfig config = FieldConfig.builder()
                .type(FieldType.KEYWORD)
                .tokenized(false)
                .build();

        assertTrue(config.isStored());
        assertTrue(config.isIndexed());
        assertFalse(config.isTokenized());
        assertEquals(FieldType.KEYWORD, config.getType());
    }

    @Test
    void testNumericConfig() {
        FieldConfig config = FieldConfig.builder()
                .type(FieldType.LONG)
                .tokenized(false)
                .build();

        assertTrue(config.isStored());
        assertTrue(config.isIndexed());
        assertFalse(config.isTokenized());
        assertEquals(FieldType.LONG, config.getType());
    }

    @Test
    void testStoredOnlyConfig() {
        FieldConfig config = FieldConfig.builder()
                .stored(true)
                .indexed(false)
                .tokenized(false)
                .build();

        assertTrue(config.isStored());
        assertFalse(config.isIndexed());
    }

    @Test
    void testIndexedOnlyConfig() {
        FieldConfig config = FieldConfig.builder()
                .stored(false)
                .indexed(true)
                .tokenized(false)
                .build();

        assertFalse(config.isStored());
        assertTrue(config.isIndexed());
    }

    @Test
    void testFluentAPI() {
        FieldConfig config = FieldConfig.builder()
                .stored(false)
                .indexed(true)
                .tokenized(false)
                .type(FieldType.KEYWORD)
                .build();

        assertFalse(config.isStored());
        assertTrue(config.isIndexed());
        assertFalse(config.isTokenized());
        assertEquals(FieldType.KEYWORD, config.getType());
    }

    @Test
    void testAllFieldTypes() {
        FieldType[] types = FieldType.values();

        assertTrue(types.length >= 10);

        // Verify specific types exist
        assertNotNull(FieldType.TEXT);
        assertNotNull(FieldType.KEYWORD);
        assertNotNull(FieldType.LONG);
        assertNotNull(FieldType.INTEGER);
        assertNotNull(FieldType.DOUBLE);
        assertNotNull(FieldType.FLOAT);
        assertNotNull(FieldType.BOOLEAN);
        assertNotNull(FieldType.DATE);
        assertNotNull(FieldType.BINARY);
        assertNotNull(FieldType.OBJECT);
    }

    @Test
    void testConfigImmutability() {
        FieldConfig config1 = FieldConfig.builder().build();
        FieldConfig config2 = FieldConfig.builder().stored(false).build();

        // New builder builds a different instance; original remains unchanged
        assertNotSame(config1, config2);
        assertTrue(config1.isStored());
        assertFalse(config2.isStored());
    }
}
