package io.synapsedb.aggregation;

import io.synapsedb.document.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static io.synapsedb.aggregation.AggregationPipeline.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for AggregationPipeline
 *
 * @author Amit Tiwari
 */
class AggregationPipelineTest {

    private List<Document> documents;

    @BeforeEach
    void setUp() {
        documents = new ArrayList<>();

        // Create sample e-commerce data
        documents.add(createProduct("1", "Laptop", "Electronics", 1000.0, 5));
        documents.add(createProduct("2", "Mouse", "Electronics", 25.0, 100));
        documents.add(createProduct("3", "Keyboard", "Electronics", 75.0, 50));
        documents.add(createProduct("4", "Chair", "Furniture", 200.0, 20));
        documents.add(createProduct("5", "Desk", "Furniture", 500.0, 10));
        documents.add(createProduct("6", "Book", "Books", 15.0, 200));
        documents.add(createProduct("7", "Pen", "Stationery", 2.0, 500));
    }

    private Document createProduct(String id, String name, String category, double price, int stock) {
        Document doc = new Document(id);
        doc.addField("name", name);
        doc.addField("category", category);
        doc.addField("price", price);
        doc.addField("stock", stock);
        return doc;
    }

    @Test
    @DisplayName("Should filter documents with match stage")
    void testMatchStage() {
        AggregationPipeline pipeline = new AggregationPipeline()
                .addStage(match("category", "Electronics"));

        AggregationPipeline.AggregationResult result = pipeline.execute(documents);

        assertEquals(3, result.getCount());
        result.getDocuments().forEach(doc ->
                assertEquals("Electronics", doc.getField("category")));
    }

    @Test
    @DisplayName("Should group and count documents")
    void testGroupWithCount() {
        AggregationPipeline pipeline = new AggregationPipeline()
                .addStage(group("category").count("total"));

        AggregationPipeline.AggregationResult result = pipeline.execute(documents);

        assertEquals(4, result.getCount()); // 4 categories

        // Find Electronics group
        Document electronicsGroup = result.getDocuments().stream()
                .filter(doc -> "Electronics".equals(doc.getField("_id")))
                .findFirst()
                .orElse(null);

        assertNotNull(electronicsGroup);
        assertEquals(3, electronicsGroup.getField("total"));
    }

    @Test
    @DisplayName("Should calculate sum in group")
    void testGroupWithSum() {
        AggregationPipeline pipeline = new AggregationPipeline()
                .addStage(group("category").sum("totalPrice", "price"));

        AggregationPipeline.AggregationResult result = pipeline.execute(documents);

        Document electronicsGroup = result.getDocuments().stream()
                .filter(doc -> "Electronics".equals(doc.getField("_id")))
                .findFirst()
                .orElse(null);

        assertNotNull(electronicsGroup);
        assertEquals(1100.0, (Double) electronicsGroup.getField("totalPrice"), 0.01);
    }

    @Test
    @DisplayName("Should calculate average in group")
    void testGroupWithAvg() {
        AggregationPipeline pipeline = new AggregationPipeline()
                .addStage(group("category").avg("avgPrice", "price"));

        AggregationPipeline.AggregationResult result = pipeline.execute(documents);

        Document electronicsGroup = result.getDocuments().stream()
                .filter(doc -> "Electronics".equals(doc.getField("_id")))
                .findFirst()
                .orElse(null);

        assertNotNull(electronicsGroup);
        double avgPrice = (Double) electronicsGroup.getField("avgPrice");
        assertEquals(366.67, avgPrice, 1.0); // (1000 + 25 + 75) / 3
    }

    @Test
    @DisplayName("Should find min value in group")
    void testGroupWithMin() {
        AggregationPipeline pipeline = new AggregationPipeline()
                .addStage(group("category").min("minPrice", "price"));

        AggregationPipeline.AggregationResult result = pipeline.execute(documents);

        Document electronicsGroup = result.getDocuments().stream()
                .filter(doc -> "Electronics".equals(doc.getField("_id")))
                .findFirst()
                .orElse(null);

        assertNotNull(electronicsGroup);
        assertEquals(25.0, electronicsGroup.getField("minPrice"));
    }

    @Test
    @DisplayName("Should find max value in group")
    void testGroupWithMax() {
        AggregationPipeline pipeline = new AggregationPipeline()
                .addStage(group("category").max("maxPrice", "price"));

        AggregationPipeline.AggregationResult result = pipeline.execute(documents);

        Document electronicsGroup = result.getDocuments().stream()
                .filter(doc -> "Electronics".equals(doc.getField("_id")))
                .findFirst()
                .orElse(null);

        assertNotNull(electronicsGroup);
        assertEquals(1000.0, electronicsGroup.getField("maxPrice"));
    }

    @Test
    @DisplayName("Should sort documents ascending")
    void testSortAscending() {
        AggregationPipeline pipeline = new AggregationPipeline()
                .addStage(sort("price", true));

        AggregationPipeline.AggregationResult result = pipeline.execute(documents);

        assertEquals("Pen", result.getDocuments().get(0).getField("name"));
        assertEquals("Laptop", result.getDocuments().get(6).getField("name"));
    }

    @Test
    @DisplayName("Should sort documents descending")
    void testSortDescending() {
        AggregationPipeline pipeline = new AggregationPipeline()
                .addStage(sort("price", false));

        AggregationPipeline.AggregationResult result = pipeline.execute(documents);

        assertEquals("Laptop", result.getDocuments().get(0).getField("name"));
        assertEquals("Pen", result.getDocuments().get(6).getField("name"));
    }

    @Test
    @DisplayName("Should limit number of results")
    void testLimit() {
        AggregationPipeline pipeline = new AggregationPipeline()
                .addStage(limit(3));

        AggregationPipeline.AggregationResult result = pipeline.execute(documents);

        assertEquals(3, result.getCount());
    }

    @Test
    @DisplayName("Should project specific fields")
    void testProject() {
        AggregationPipeline pipeline = new AggregationPipeline()
                .addStage(project("name", "price"));

        AggregationPipeline.AggregationResult result = pipeline.execute(documents);

        Document firstDoc = result.getDocuments().get(0);
        assertTrue(firstDoc.hasField("name"));
        assertTrue(firstDoc.hasField("price"));
        assertFalse(firstDoc.hasField("category"));
        assertFalse(firstDoc.hasField("stock"));
    }

    @Test
    @DisplayName("Should chain multiple stages")
    void testMultipleStages() {
        AggregationPipeline pipeline = new AggregationPipeline()
                .addStage(match("category", "Electronics"))
                .addStage(sort("price", false))
                .addStage(limit(2))
                .addStage(project("name", "price"));

        AggregationPipeline.AggregationResult result = pipeline.execute(documents);

        assertEquals(2, result.getCount());
        assertEquals("Laptop", result.getDocuments().get(0).getField("name"));
        assertEquals("Keyboard", result.getDocuments().get(1).getField("name"));
        assertFalse(result.getDocuments().get(0).hasField("category"));
    }

    @Test
    @DisplayName("Should handle complex aggregation pipeline")
    void testComplexPipeline() {
        // Find top 2 categories by total inventory value
        AggregationPipeline pipeline = new AggregationPipeline()
                .addStage(group("category")
                        .sum("totalValue", "price")
                        .count("productCount"))
                .addStage(sort("totalValue", false))
                .addStage(limit(2));

        AggregationPipeline.AggregationResult result = pipeline.execute(documents);

        assertEquals(2, result.getCount());
        Document topCategory = result.getDocuments().get(0);
        assertNotNull(topCategory.getField("_id"));
        assertNotNull(topCategory.getField("totalValue"));
        assertNotNull(topCategory.getField("productCount"));
    }

    @Test
    @DisplayName("Should handle empty input")
    void testEmptyInput() {
        AggregationPipeline pipeline = new AggregationPipeline()
                .addStage(match("category", "Electronics"));

        AggregationPipeline.AggregationResult result = pipeline.execute(new ArrayList<>());

        assertEquals(0, result.getCount());
    }

    @Test
    @DisplayName("Should handle group with multiple aggregations")
    void testMultipleAggregations() {
        AggregationPipeline pipeline = new AggregationPipeline()
                .addStage(group("category")
                        .count("count")
                        .sum("totalPrice", "price")
                        .avg("avgPrice", "price")
                        .min("minPrice", "price")
                        .max("maxPrice", "price"));

        AggregationPipeline.AggregationResult result = pipeline.execute(documents);

        Document electronicsGroup = result.getDocuments().stream()
                .filter(doc -> "Electronics".equals(doc.getField("_id")))
                .findFirst()
                .orElse(null);

        assertNotNull(electronicsGroup);
        assertEquals(3, electronicsGroup.getField("count"));
        assertEquals(1100.0, (Double) electronicsGroup.getField("totalPrice"), 0.01);
        assertTrue((Double) electronicsGroup.getField("avgPrice") > 0);
        assertEquals(25.0, electronicsGroup.getField("minPrice"));
        assertEquals(1000.0, electronicsGroup.getField("maxPrice"));
    }
}

