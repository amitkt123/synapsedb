package io.synapsedb.core.examples.document;

import io.synapsedb.core.document.FieldConfig;
import io.synapsedb.core.document.FieldType;
import io.synapsedb.core.document.mapper.DocumentConverter;
import org.apache.lucene.document.*;
import org.apache.lucene.document.Document;

import java.util.Date;
import java.util.List;

/**
 * Example usages for SynapseDB Document conversion utilities
 * @author Amit Tiwari
 */
public class Main {

    public static void main(String[] args) {
        // Example 1: Create SynapseDoc and convert to Lucene
        exampleSynapseToLucene();

        // Example 2: Convert Lucene to SynapseDoc
        exampleLuceneToSynapse();

        // Example 3: Complex document with multiple field types
        exampleComplexDocument();
    }

    private static void exampleSynapseToLucene() {
        System.out.println("=== Example 1: SynapseDoc to Lucene ===");

        // Create SynapseDoc
        io.synapsedb.core.document.Document synapseDoc = new io.synapsedb.core.document.Document("user-123")
                .addField("name", "John Doe", FieldConfig.builder().type(FieldType.TEXT).tokenized(true).build())
                .addField("email", "john@example.com", FieldConfig.builder().type(FieldType.KEYWORD).tokenized(false).build())
                .addField("age", 30, FieldConfig.builder().type(FieldType.INTEGER).tokenized(false).build())
                .addField("bio", "Software engineer passionate about search engines",
                        FieldConfig.builder().stored(false).indexed(true).tokenized(true).build());

        // Add multi-valued field (tags)
        synapseDoc.addFields("tags", List.of("java", "lucene", "search"),
                FieldConfig.builder().type(FieldType.KEYWORD).tokenized(false).build());
        System.out.println(synapseDoc.toString());
        // Convert to Lucene
        Document luceneDoc = DocumentConverter.toLuceneDocument(synapseDoc);

        System.out.println("Lucene document created with " +
                luceneDoc.getFields().size() + " fields");
        System.out.println("ID: " + luceneDoc.get("_id"));
        System.out.println();
    }

    private static void exampleLuceneToSynapse() {
        System.out.println("=== Example 2: Lucene to SynapseDoc ===");

        // Simulate retrieving Lucene document from search
        org.apache.lucene.document.Document luceneDoc = new org.apache.lucene.document.Document();
        luceneDoc.add(new StringField("_id", "doc-456", Field.Store.YES));
        luceneDoc.add(new TextField("title", "SynapseDB Guide", Field.Store.YES));
        luceneDoc.add(new StringField("author", "Alice", Field.Store.YES));
        luceneDoc.add(new IntPoint("views", 1000));
        luceneDoc.add(new StoredField("views", 1000));

        // Convert to SynapseDoc
        io.synapsedb.core.document.Document synapseDoc = DocumentConverter.fromLuceneDocument(luceneDoc);

        System.out.println("SynapseDoc: " + synapseDoc);
        System.out.println("Title: " + synapseDoc.getField("title"));
        System.out.println("Views: " + synapseDoc.getField("views"));
        System.out.println();
    }

    private static void exampleComplexDocument() {
        System.out.println("=== Example 3: Complex Document ===");

        io.synapsedb.core.document.Document product = new io.synapsedb.core.document.Document("product-789")
                .addField("name", "Laptop", FieldConfig.builder().type(FieldType.TEXT).tokenized(true).build())
                .addField("sku", "LAPTOP-001", FieldConfig.builder().type(FieldType.KEYWORD).tokenized(false).build())
                .addField("price", 999.99, FieldConfig.builder().type(FieldType.DOUBLE).tokenized(false).build())
                .addField("inStock", true, FieldConfig.builder().type(FieldType.KEYWORD).tokenized(false).build())
                .addField("createdAt", new Date(),
                        FieldConfig.builder().type(FieldType.DATE).tokenized(false).build())
                .addField("description", "High-performance laptop for developers",
                        FieldConfig.builder().type(FieldType.TEXT).tokenized(true).build());

        // Add categories
        product.addFields("categories",
                List.of("Electronics", "Computers", "Laptops"),
                FieldConfig.builder().type(FieldType.KEYWORD).tokenized(false).build());

        // Convert and back
        Document luceneDoc = DocumentConverter.toLuceneDocument(product);
        io.synapsedb.core.document.Document recovered = DocumentConverter.fromLuceneDocument(luceneDoc);

        System.out.println("Original: " + product);
        System.out.println("Recovered: " + recovered);
        System.out.println("Categories: " + recovered.getFields("categories"));
    }
}
