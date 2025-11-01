package io.synapsedb.examples.mapping;

import io.synapsedb.index.mapping.*;

/**
 * Examples demonstrating how to use MappingBuilder to create index mappings.
 *
 * @author Amit Tiwari
 */
public class MappingBuilderExamples {

    /**
     * Example 1: Simple blog post mapping using convenience methods
     */
    public static IndexMapping createBlogPostMapping() {
        return MappingBuilder.create()
            .addTextField("title", true, "standard")
            .addTextField("content", true, "english")
            .addKeywordField("author", true)
            .addKeywordField("category", true)
            .addDateField("published_at", false, true)
            .addLongField("view_count", true)
            .addBooleanField("is_published", true)
            .dynamicStrict()
            .build();
    }

    /**
     * Example 2: E-commerce product mapping
     */
    public static IndexMapping createProductMapping() {
        return MappingBuilder.create()
            .addTextField("name", true)
            .addTextField("description", true, "standard")
            .addKeywordField("sku", true, true)
            .addKeywordField("brand", true, true)
            .addKeywordField("category", true, true)
            .addDoubleField("price", true, true)
            .addDoubleField("rating", false, true)
            .addLongField("stock_quantity", true)
            .addDateField("created_at", false, true)
            .addDateField("updated_at", false, true)
            .addBooleanField("in_stock", true)
            .addBooleanField("featured", true)
            .dynamicDisabled()
            .build();
    }

    /**
     * Example 3: User profile mapping with fine-grained control
     */
    public static IndexMapping createUserProfileMapping() {
        return MappingBuilder.create()
            .field("username", FieldMapping.FieldType.KEYWORD)
                .stored(true)
                .indexed(true)
                .docValues(true)
                .end()
            .field("email", FieldMapping.FieldType.KEYWORD)
                .stored(true)
                .indexed(true)
                .end()
            .field("full_name", FieldMapping.FieldType.TEXT)
                .stored(true)
                .indexed(true)
                .analyzer("standard")
                .end()
            .field("bio", FieldMapping.FieldType.TEXT)
                .stored(true)
                .analyzer("english")
                .end()
            .addDateField("registered_at", false, true)
            .addDateField("last_login", false, true)
            .addLongField("login_count", true)
            .addBooleanField("is_active", true)
            .addBooleanField("is_verified", true)
            .dynamicEnabled()
            .build();
    }

    /**
     * Example 4: Log entry mapping (time-series data)
     */
    public static IndexMapping createLogEntryMapping() {
        return MappingBuilder.create()
            .addDateField("timestamp", false, true)
            .addKeywordField("level", true, true)  // INFO, WARN, ERROR
            .addKeywordField("logger", true, true)
            .addTextField("message", true, "standard")
            .addKeywordField("thread", false, true)
            .addKeywordField("host", false, true)
            .addLongField("duration_ms", true)
            .dynamicEnabled()  // Allow additional fields for context
            .build();
    }

    /**
     * Example 5: Minimal mapping with defaults
     */
    public static IndexMapping createMinimalMapping() {
        return MappingBuilder.create()
            .addTextField("title")
            .addKeywordField("id")
            .addDateField("timestamp")
            .build();
    }

    /**
     * Example 6: Complex chained field building
     */
    public static IndexMapping createComplexMapping() {
        return MappingBuilder.create()
            .field("field1", FieldMapping.FieldType.TEXT)
                .stored(true)
                .analyzer("standard")
                .field("field2", FieldMapping.FieldType.KEYWORD)
                    .stored(false)
                    .docValues(true)
                    .field("field3", FieldMapping.FieldType.LONG)
                        .stored(true)
                        .docValues(true)
                        .build();
    }

    /**
     * Example 7: Adding and removing fields
     */
    public static IndexMapping createDynamicMapping() {
        return MappingBuilder.create()
            .addTextField("title", true)
            .addKeywordField("temp_field", true)
            .addLongField("count", true)
            .removeField("temp_field")  // Remove the temporary field
            .dynamicStrict()
            .build();
    }

    /**
     * Example 8: Document metadata mapping
     */
    public static IndexMapping createDocumentMetadataMapping() {
        return MappingBuilder.create()
            .addKeywordField("doc_id", true, true)
            .addTextField("title", true, "standard")
            .addTextField("author", true)
            .addKeywordField("doc_type", true, true)
            .addKeywordField("mime_type", false, true)
            .addLongField("file_size_bytes", true)
            .addDateField("created_date", false, true)
            .addDateField("modified_date", false, true)
            .addKeywordField("tags", false, true)
            .addTextField("content", false)  // Not stored, only indexed
            .dynamicDisabled()
            .build();
    }

    public static void main(String[] args) {
        // Demonstrate all examples
        System.out.println("=== Blog Post Mapping ===");
        IndexMapping blogMapping = createBlogPostMapping();
        System.out.println(blogMapping);
        System.out.println("Fields: " + blogMapping.fields().keySet());
        System.out.println("Dynamic: " + blogMapping.dynamic());
        System.out.println();

        System.out.println("=== Product Mapping ===");
        IndexMapping productMapping = createProductMapping();
        System.out.println(productMapping);
        System.out.println("Fields: " + productMapping.fields().keySet());
        System.out.println();

        System.out.println("=== User Profile Mapping ===");
        IndexMapping userMapping = createUserProfileMapping();
        System.out.println(userMapping);
        System.out.println("Fields: " + userMapping.fields().keySet());
        System.out.println();

        System.out.println("=== Log Entry Mapping ===");
        IndexMapping logMapping = createLogEntryMapping();
        System.out.println(logMapping);
        System.out.println("Fields: " + logMapping.fields().keySet());
        System.out.println();

        System.out.println("=== Minimal Mapping ===");
        IndexMapping minimalMapping = createMinimalMapping();
        System.out.println(minimalMapping);
        System.out.println("Fields: " + minimalMapping.fields().keySet());
        System.out.println();

        // Demonstrate field details
        System.out.println("=== Field Details Example ===");
        blogMapping.field("title").ifPresent(field -> {
            System.out.println("Title field:");
            System.out.println("  Type: " + field.type());
            System.out.println("  Stored: " + field.stored());
            System.out.println("  Indexed: " + field.indexed());
            System.out.println("  Analyzer: " + field.analyzer().orElse("none"));
        });
    }
}

