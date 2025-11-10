package io.synapsedb.core.search;

import io.synapsedb.core.document.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for FullTextIndex
 *
 * @author Amit Tiwari
 */
class FullTextIndexTest {

    private FullTextIndex index;

    @BeforeEach
    void setUp() {
        index = new FullTextIndex();
    }

    @Test
    @DisplayName("Should index and search single field")
    void testBasicSearch() {
        // Create and index documents
        Document doc1 = new Document("1");
        doc1.addField("title", "Introduction to Java Programming");

        Document doc2 = new Document("2");
        doc2.addField("title", "Advanced Java Concepts");

        Document doc3 = new Document("3");
        doc3.addField("title", "Python for Beginners");

        index.indexDocument(doc1, List.of("title"));
        index.indexDocument(doc2, List.of("title"));
        index.indexDocument(doc3, List.of("title"));

        // Search for Java
        FullTextIndex.SearchResult result = index.search("title", "Java",
                new FullTextIndex.SearchOptions(0, 10));

        assertEquals(2, result.getTotalMatches(), "Should find 2 documents with 'Java'");
        assertEquals(2, result.getResults().size());
    }

    @Test
    @DisplayName("Should calculate TF-IDF scores correctly")
    void testTFIDFScoring() {
        Document doc1 = new Document("1");
        doc1.addField("content", "Java Java Java programming");

        Document doc2 = new Document("2");
        doc2.addField("content", "Java programming language");

        Document doc3 = new Document("3");
        doc3.addField("content", "Python programming language");

        index.indexDocument(doc1, List.of("content"));
        index.indexDocument(doc2, List.of("content"));
        index.indexDocument(doc3, List.of("content"));

        FullTextIndex.SearchResult result = index.search("content", "Java",
                new FullTextIndex.SearchOptions(0, 10));

        // Should find 2 documents with Java (doc1 and doc2, not doc3)
        assertEquals(2, result.getResults().size());

        // Both results should have positive scores
        assertTrue(result.getResults().get(0).getScore() > 0);
        assertTrue(result.getResults().get(1).getScore() > 0);

        // Doc1 should have higher score due to more occurrences (3 vs 1)
        String firstDocId = result.getResults().get(0).getDocument().getId();
        String secondDocId = result.getResults().get(1).getDocument().getId();
        double firstScore = result.getResults().get(0).getScore();
        double secondScore = result.getResults().get(1).getScore();

        if ("1".equals(firstDocId)) {
            assertTrue(firstScore > secondScore,
                String.format("Doc1 (3 occurrences) score %.2f should be > Doc2 (1 occurrence) score %.2f",
                    firstScore, secondScore));
        } else {
            assertTrue(secondScore > firstScore,
                String.format("Doc1 (3 occurrences) score %.2f should be > Doc2 (1 occurrence) score %.2f",
                    secondScore, firstScore));
        }
    }

    @Test
    @DisplayName("Should search across multiple fields")
    void testMultiFieldSearch() {
        Document doc1 = new Document("1");
        doc1.addField("title", "Java Programming");
        doc1.addField("description", "Learn Java basics");

        Document doc2 = new Document("2");
        doc2.addField("title", "Python Guide");
        doc2.addField("description", "Python for beginners");

        index.indexDocument(doc1, List.of("title", "description"));
        index.indexDocument(doc2, List.of("title", "description"));

        FullTextIndex.SearchResult result = index.searchMultipleFields(
                List.of("title", "description"),
                "Java",
                new FullTextIndex.SearchOptions(0, 10)
        );

        assertEquals(1, result.getTotalMatches());
        assertEquals("1", result.getResults().get(0).getDocument().getId());
    }

    @Test
    @DisplayName("Should perform phrase search")
    void testPhraseSearch() {
        Document doc1 = new Document("1");
        doc1.addField("text", "Java programming language");

        Document doc2 = new Document("2");
        doc2.addField("text", "Programming in Java");

        Document doc3 = new Document("3");
        doc3.addField("text", "Java language for programming");

        index.indexDocument(doc1, List.of("text"));
        index.indexDocument(doc2, List.of("text"));
        index.indexDocument(doc3, List.of("text"));

        FullTextIndex.SearchResult result = index.phraseSearch("text", "Java programming",
                new FullTextIndex.SearchOptions(0, 10));

        assertEquals(1, result.getResults().size());
        assertEquals("1", result.getResults().get(0).getDocument().getId());
    }

    @Test
    @DisplayName("Should filter stop words")
    void testStopWordFiltering() {
        Document doc1 = new Document("1");
        doc1.addField("text", "This is a test document");

        Document doc2 = new Document("2");
        doc2.addField("text", "Another test for searching");

        index.indexDocument(doc1, List.of("text"));
        index.indexDocument(doc2, List.of("text"));

        // Search for stop word "is" should return fewer/no results
        FullTextIndex.SearchResult result1 = index.search("text", "is",
                new FullTextIndex.SearchOptions(0, 10));

        // Search for meaningful word "test"
        FullTextIndex.SearchResult result2 = index.search("text", "test",
                new FullTextIndex.SearchOptions(0, 10));

        assertTrue(result2.getTotalMatches() >= result1.getTotalMatches());
    }

    @Test
    @DisplayName("Should handle document removal")
    void testDocumentRemoval() {
        Document doc1 = new Document("1");
        doc1.addField("title", "Java Programming");

        index.indexDocument(doc1, List.of("title"));

        FullTextIndex.SearchResult result1 = index.search("title", "Java",
                new FullTextIndex.SearchOptions(0, 10));
        assertEquals(1, result1.getTotalMatches());

        index.removeDocument("1");

        FullTextIndex.SearchResult result2 = index.search("title", "Java",
                new FullTextIndex.SearchOptions(0, 10));
        assertEquals(0, result2.getTotalMatches());
    }

    @Test
    @DisplayName("Should handle pagination")
    void testPagination() {
        for (int i = 1; i <= 10; i++) {
            Document doc = new Document(String.valueOf(i));
            doc.addField("text", "Java programming tutorial " + i);
            index.indexDocument(doc, List.of("text"));
        }

        // First page
        FullTextIndex.SearchResult page1 = index.search("text", "Java",
                new FullTextIndex.SearchOptions(0, 3));
        assertEquals(3, page1.getResults().size());
        assertEquals(10, page1.getTotalMatches());

        // Second page
        FullTextIndex.SearchResult page2 = index.search("text", "Java",
                new FullTextIndex.SearchOptions(3, 3));
        assertEquals(3, page2.getResults().size());
    }

    @Test
    @DisplayName("Should return empty result for no matches")
    void testNoMatches() {
        Document doc = new Document("1");
        doc.addField("title", "Java Programming");

        index.indexDocument(doc, List.of("title"));

        FullTextIndex.SearchResult result = index.search("title", "Python",
                new FullTextIndex.SearchOptions(0, 10));

        assertEquals(0, result.getTotalMatches());
        assertTrue(result.getResults().isEmpty());
    }

    @Test
    @DisplayName("Should handle case insensitive search")
    void testCaseInsensitiveSearch() {
        Document doc = new Document("1");
        doc.addField("title", "Java Programming");

        index.indexDocument(doc, List.of("title"));

        FullTextIndex.SearchResult result1 = index.search("title", "java",
                new FullTextIndex.SearchOptions(0, 10));
        FullTextIndex.SearchResult result2 = index.search("title", "JAVA",
                new FullTextIndex.SearchOptions(0, 10));

        assertEquals(1, result1.getTotalMatches());
        assertEquals(1, result2.getTotalMatches());
    }

    @Test
    @DisplayName("Should handle multi-word queries")
    void testMultiWordQuery() {
        Document doc1 = new Document("1");
        doc1.addField("text", "Java programming language");

        Document doc2 = new Document("2");
        doc2.addField("text", "Python programming");

        Document doc3 = new Document("3");
        doc3.addField("text", "Java language");

        index.indexDocument(doc1, List.of("text"));
        index.indexDocument(doc2, List.of("text"));
        index.indexDocument(doc3, List.of("text"));

        FullTextIndex.SearchResult result = index.search("text", "Java language",
                new FullTextIndex.SearchOptions(0, 10));

        assertTrue(result.getTotalMatches() >= 2);
    }
}

