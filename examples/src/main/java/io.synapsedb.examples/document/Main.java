package io.synapsedb.examples.document;

import io.synapsedb.document.*;
import io.synapsedb.document.mapper.DocumentConverter;
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
        io.synapsedb.document.Document synapseDoc = new io.synapsedb.document.Document("user-123")
                .addField("name", "John Doe", FieldConfig.text())
                .addField("email", "john@example.com", FieldConfig.keyword())
                .addField("age", 30, FieldConfig.number(FieldConfig.FieldType.INTEGER))
                .addField("bio", "Software engineer passionate about search engines",
                        FieldConfig.indexedOnly());

        // Add multi-valued field (tags)
        synapseDoc.addFields("tags", List.of("java", "lucene", "search"),
                FieldConfig.keyword());
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
        io.synapsedb.document.Document synapseDoc = DocumentConverter.fromLuceneDocument(luceneDoc);

        System.out.println("SynapseDoc: " + synapseDoc);
        System.out.println("Title: " + synapseDoc.getField("title"));
        System.out.println("Views: " + synapseDoc.getField("views"));
        System.out.println();
    }

    private static void exampleComplexDocument() {
        System.out.println("=== Example 3: Complex Document ===");

        io.synapsedb.document.Document product = new io.synapsedb.document.Document("product-789")
                .addField("name", "Laptop", FieldConfig.text())
                .addField("sku", "LAPTOP-001", FieldConfig.keyword())
                .addField("price", 999.99, FieldConfig.number(FieldConfig.FieldType.DOUBLE))
                .addField("inStock", true, FieldConfig.keyword())
                .addField("createdAt", new Date(),
                        FieldConfig.defaults().setType(FieldConfig.FieldType.DATE))
                .addField("description", "High-performance laptop for developers",
                        FieldConfig.text());

        // Add categories
        product.addFields("categories",
                List.of("Electronics", "Computers", "Laptops"),
                FieldConfig.keyword());

        // Convert and back
        Document luceneDoc = DocumentConverter.toLuceneDocument(product);
        io.synapsedb.document.Document recovered = DocumentConverter.fromLuceneDocument(luceneDoc);

        System.out.println("Original: " + product);
        System.out.println("Recovered: " + recovered);
        System.out.println("Categories: " + recovered.getFields("categories"));
    }
}
