package io.synapsedb.collection;

import io.synapsedb.aggregation.AggregationPipeline;
import io.synapsedb.document.Document;
import io.synapsedb.search.FullTextIndex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Collection with full-text search and aggregation
 *
 * @author Amit Tiwari
 */
class CollectionTest {

    private Collection collection;

    @BeforeEach
    void setUp() {
        collection = new Collection("testCollection");
    }

    @Test
    @DisplayName("Should insert and retrieve documents")
    void testBasicOperations() {
        Document doc = new Document();
        doc.addField("name", "John");
        doc.addField("age", 30);

        collection.insert(doc);

        assertNotNull(doc.getId());
        assertEquals(1, collection.count());

        Document retrieved = collection.findById(doc.getId());
        assertNotNull(retrieved);
        assertEquals("John", retrieved.getField("name"));
    }

    @Test
    @DisplayName("Should update documents")
    void testUpdate() {
        Document doc = new Document("123");
        doc.addField("name", "John");
        collection.insert(doc);

        Map<String, Object> updates = new HashMap<>();
        updates.put("name", "Jane");
        updates.put("city", "New York");

        assertTrue(collection.update("123", updates));

        Document updated = collection.findById("123");
        assertEquals("Jane", updated.getField("name"));
        assertEquals("New York", updated.getField("city"));
    }

    @Test
    @DisplayName("Should delete documents")
    void testDelete() {
        Document doc = new Document("123");
        doc.addField("name", "John");
        collection.insert(doc);

        assertEquals(1, collection.count());

        assertTrue(collection.delete("123"));
        assertEquals(0, collection.count());
        assertNull(collection.findById("123"));
    }

    @Test
    @DisplayName("Should find documents by field value")
    void testFindByField() {
        Document doc1 = new Document("1");
        doc1.addField("category", "Electronics");
        doc1.addField("name", "Laptop");

        Document doc2 = new Document("2");
        doc2.addField("category", "Electronics");
        doc2.addField("name", "Mouse");

        Document doc3 = new Document("3");
        doc3.addField("category", "Books");
        doc3.addField("name", "Java Guide");

        collection.insert(doc1);
        collection.insert(doc2);
        collection.insert(doc3);

        List<Document> electronics = collection.find("category", "Electronics");
        assertEquals(2, electronics.size());
    }

    @Test
    @DisplayName("Should perform full-text search")
    void testFullTextSearch() {
        collection.enableFullTextSearch("title", "description");

        Document doc1 = new Document("1");
        doc1.addField("title", "Introduction to Java Programming");
        doc1.addField("description", "Learn Java basics");

        Document doc2 = new Document("2");
        doc2.addField("title", "Advanced Python Techniques");
        doc2.addField("description", "Master Python programming");

        Document doc3 = new Document("3");
        doc3.addField("title", "Java Design Patterns");
        doc3.addField("description", "Common patterns in Java");

        collection.insert(doc1);
        collection.insert(doc2);
        collection.insert(doc3);

        FullTextIndex.SearchResult result = collection.search("title", "Java", 10);
        assertEquals(2, result.getTotalMatches());
    }

    @Test
    @DisplayName("Should search multiple fields")
    void testMultiFieldSearch() {
        collection.enableFullTextSearch("title", "description");

        Document doc1 = new Document("1");
        doc1.addField("title", "Java Programming");
        doc1.addField("description", "Learn basics");

        Document doc2 = new Document("2");
        doc2.addField("title", "Python Guide");
        doc2.addField("description", "Java comparison included");

        collection.insert(doc1);
        collection.insert(doc2);

        FullTextIndex.SearchResult result = collection.searchMultiple(
                List.of("title", "description"), "Java", 10);
        assertEquals(2, result.getTotalMatches());
    }

    @Test
    @DisplayName("Should perform phrase search")
    void testPhraseSearch() {
        collection.enableFullTextSearch("content");

        Document doc1 = new Document("1");
        doc1.addField("content", "Java programming language");

        Document doc2 = new Document("2");
        doc2.addField("content", "Programming in Java");

        collection.insert(doc1);
        collection.insert(doc2);

        FullTextIndex.SearchResult result = collection.phraseSearch("content", "Java programming", 10);
        assertEquals(1, result.getResults().size());
        assertEquals("1", result.getResults().get(0).getDocument().getId());
    }

    @Test
    @DisplayName("Should execute aggregation pipeline")
    void testAggregation() {
        // Insert products
        collection.insert(createProduct("1", "Laptop", "Electronics", 1000.0));
        collection.insert(createProduct("2", "Mouse", "Electronics", 25.0));
        collection.insert(createProduct("3", "Chair", "Furniture", 200.0));

        AggregationPipeline pipeline = new AggregationPipeline()
                .addStage(AggregationPipeline.group("category")
                        .count("total")
                        .avg("avgPrice", "price"));

        AggregationPipeline.AggregationResult result = collection.aggregate(pipeline);

        assertEquals(2, result.getCount()); // 2 categories

        Document electronicsGroup = result.getDocuments().stream()
                .filter(doc -> "Electronics".equals(doc.getField("_id")))
                .findFirst()
                .orElse(null);

        assertNotNull(electronicsGroup);
        assertEquals(2, electronicsGroup.getField("total"));
    }

    @Test
    @DisplayName("Should get collection statistics")
    void testGetStats() {
        collection.insert(new Document("1"));
        collection.insert(new Document("2"));
        collection.enableFullTextSearch("field1", "field2");

        Collection.CollectionStats stats = collection.getStats();

        assertEquals("testCollection", stats.getName());
        assertEquals(2, stats.getDocumentCount());
        assertEquals(2, stats.getSearchableFieldsCount());
    }

    @Test
    @DisplayName("Should handle insert many")
    void testInsertMany() {
        List<Document> docs = List.of(
                new Document("1"),
                new Document("2"),
                new Document("3")
        );

        collection.insertMany(docs);
        assertEquals(3, collection.count());
    }

    @Test
    @DisplayName("Should auto-generate IDs if not provided")
    void testAutoGenerateId() {
        Document doc = new Document();
        doc.addField("name", "Test");

        assertNull(doc.getId());
        collection.insert(doc);
        assertNotNull(doc.getId());
    }

    @Test
    @DisplayName("Should re-index on update when full-text enabled")
    void testReIndexOnUpdate() {
        collection.enableFullTextSearch("title");

        Document doc = new Document("1");
        doc.addField("title", "Java Programming");
        collection.insert(doc);

        FullTextIndex.SearchResult result1 = collection.search("title", "Java", 10);
        assertEquals(1, result1.getTotalMatches());

        // Update title
        Map<String, Object> updates = new HashMap<>();
        updates.put("title", "Python Programming");
        collection.update("1", updates);

        // Should find with Python, not Java
        FullTextIndex.SearchResult result2 = collection.search("title", "Python", 10);
        assertEquals(1, result2.getTotalMatches());

        FullTextIndex.SearchResult result3 = collection.search("title", "Java", 10);
        assertEquals(0, result3.getTotalMatches());
    }

    @Test
    @DisplayName("Should remove from index on delete")
    void testRemoveFromIndexOnDelete() {
        collection.enableFullTextSearch("title");

        Document doc = new Document("1");
        doc.addField("title", "Java Programming");
        collection.insert(doc);

        FullTextIndex.SearchResult result1 = collection.search("title", "Java", 10);
        assertEquals(1, result1.getTotalMatches());

        collection.delete("1");

        FullTextIndex.SearchResult result2 = collection.search("title", "Java", 10);
        assertEquals(0, result2.getTotalMatches());
    }

    private Document createProduct(String id, String name, String category, double price) {
        Document doc = new Document(id);
        doc.addField("name", name);
        doc.addField("category", category);
        doc.addField("price", price);
        return doc;
    }
}

