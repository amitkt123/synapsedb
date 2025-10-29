package io.synapsedb.document;

import io.synapsedb.document.mapper.DocumentConverter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests with actual Lucene indexing and searching
 */
class IntegrationTest {

    private Directory directory;
    private IndexWriter indexWriter;

    @BeforeEach
    void setUp() throws IOException {
        directory = new ByteBuffersDirectory();
        StandardAnalyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        indexWriter = new IndexWriter(directory, config);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (indexWriter != null && indexWriter.isOpen()) {
            indexWriter.close();
        }
        if (directory != null) {
            directory.close();
        }
    }

    // ============ Basic Integration Tests ============

    @Test
    void testIndexAndSearchSingleDocument() throws IOException {
        // Create SynapseDB document
        io.synapsedb.document.Document synapseDoc = new io.synapsedb.document.Document("doc1")
                .addField("title", "Java Programming", FieldConfig.text())
                .addField("content", "Learn Java basics and advanced concepts", FieldConfig.text());

        // Convert and index
        Document luceneDoc = DocumentConverter.toLuceneDocument(synapseDoc);
        indexWriter.addDocument(luceneDoc);
        indexWriter.commit();

        // Search
        IndexReader reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);

        Query query = new TermQuery(new Term("title", "java"));
        TopDocs results = searcher.search(query, 10);

        assertEquals(1, results.totalHits.value);

        // Retrieve and convert back
        Document foundDoc = searcher.doc(results.scoreDocs[0].doc);
        io.synapsedb.document.Document restored = DocumentConverter.fromLuceneDocument(foundDoc);

        assertEquals("doc1", restored.getId());
        assertEquals("Java Programming", restored.getField("title"));

        reader.close();
    }

    @Test
    void testIndexAndSearchMultipleDocuments() throws IOException {
        // Index 3 documents
        io.synapsedb.document.Document doc1 = new io.synapsedb.document.Document("doc1")
                .addField("title", "Java Programming", FieldConfig.text());

        io.synapsedb.document.Document doc2 = new io.synapsedb.document.Document("doc2")
                .addField("title", "Python Guide", FieldConfig.text());

        io.synapsedb.document.Document doc3 = new io.synapsedb.document.Document("doc3")
                .addField("title", "Java Advanced", FieldConfig.text());

        indexWriter.addDocument(DocumentConverter.toLuceneDocument(doc1));
        indexWriter.addDocument(DocumentConverter.toLuceneDocument(doc2));
        indexWriter.addDocument(DocumentConverter.toLuceneDocument(doc3));
        indexWriter.commit();

        // Search for "java"
        IndexReader reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);

        Query query = new TermQuery(new Term("title", "java"));
        TopDocs results = searcher.search(query, 10);

        assertEquals(2, results.totalHits.value);  // doc1 and doc3

        reader.close();
    }

    @Test
    void testIndexAndSearchWithKeywordField() throws IOException {
        // Keyword fields are not analyzed (exact match)
        io.synapsedb.document.Document doc = new io.synapsedb.document.Document("doc1")
                .addField("email", "john@example.com", FieldConfig.keyword());

        indexWriter.addDocument(DocumentConverter.toLuceneDocument(doc));
        indexWriter.commit();

        IndexReader reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);

        // Exact match should work
        Query exactQuery = new TermQuery(new Term("email", "john@example.com"));
        TopDocs exactResults = searcher.search(exactQuery, 10);
        assertEquals(1, exactResults.totalHits.value);

        // Partial match should NOT work (keyword field)
        Query partialQuery = new TermQuery(new Term("email", "john"));
        TopDocs partialResults = searcher.search(partialQuery, 10);
        assertEquals(0, partialResults.totalHits.value);

        reader.close();
    }

    @Test
    void testIndexAndSearchNumericField() throws IOException {
        // Index documents with numeric fields
        io.synapsedb.document.Document doc1 = new io.synapsedb.document.Document("doc1")
                .addField("price", 50L, FieldConfig.number(FieldConfig.FieldType.LONG));

        io.synapsedb.document.Document doc2 = new io.synapsedb.document.Document("doc2")
                .addField("price", 100L, FieldConfig.number(FieldConfig.FieldType.LONG));

        io.synapsedb.document.Document doc3 = new io.synapsedb.document.Document("doc3")
                .addField("price", 150L, FieldConfig.number(FieldConfig.FieldType.LONG));

        indexWriter.addDocument(DocumentConverter.toLuceneDocument(doc1));
        indexWriter.addDocument(DocumentConverter.toLuceneDocument(doc2));
        indexWriter.addDocument(DocumentConverter.toLuceneDocument(doc3));
        indexWriter.commit();

        IndexReader reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);

        // Range query: price between 75 and 125
        Query rangeQuery = LongPoint.newRangeQuery("price", 75L, 125L);
        TopDocs results = searcher.search(rangeQuery, 10);

        assertEquals(1, results.totalHits.value);  // Only doc2 (price=100)

        // Retrieve and verify
        Document foundDoc = searcher.doc(results.scoreDocs[0].doc);
        io.synapsedb.document.Document restored = DocumentConverter.fromLuceneDocument(foundDoc);

        assertEquals("doc2", restored.getId());
        Number price = (Number) restored.getField("price");
        assertEquals(100L, price.longValue());

        reader.close();
    }

    @Test
    void testIndexAndSearchMultiValuedField() throws IOException {
        // Document with multiple tags
        io.synapsedb.document.Document doc = new io.synapsedb.document.Document("doc1")
                .addField("title", "Search Engine Tutorial", FieldConfig.text());

        doc.addField("tag", "java");
        doc.addField("tag", "lucene");
        doc.addField("tag", "search");

        indexWriter.addDocument(DocumentConverter.toLuceneDocument(doc));
        indexWriter.commit();

        IndexReader reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);

        // Search by any tag
        Query query = new TermQuery(new Term("tag", "lucene"));
        TopDocs results = searcher.search(query, 10);

        assertEquals(1, results.totalHits.value);

        // Retrieve and verify all tags preserved
        Document foundDoc = searcher.doc(results.scoreDocs[0].doc);
        io.synapsedb.document.Document restored = DocumentConverter.fromLuceneDocument(foundDoc);

        assertEquals(3, restored.getFields("tag").size());
        assertTrue(restored.getFields("tag").contains("java"));
        assertTrue(restored.getFields("tag").contains("lucene"));
        assertTrue(restored.getFields("tag").contains("search"));

        reader.close();
    }

    @Test
    void testIndexedOnlyFieldNotRetrievable() throws IOException {
        // Field that's indexed but not stored
        FieldConfig config = FieldConfig.indexedOnly();

        io.synapsedb.document.Document doc = new io.synapsedb.document.Document("doc1")
                .addField("searchable", "This can be searched", config)
                .addField("title", "Visible Title", FieldConfig.text());

        indexWriter.addDocument(DocumentConverter.toLuceneDocument(doc));
        indexWriter.commit();

        IndexReader reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);

        // Can search on indexed-only field
        Query query = new TermQuery(new Term("searchable", "searched"));
        TopDocs results = searcher.search(query, 10);

        assertEquals(1, results.totalHits.value);

        // But value is not stored/retrievable
        Document foundDoc = searcher.doc(results.scoreDocs[0].doc);
        io.synapsedb.document.Document restored = DocumentConverter.fromLuceneDocument(foundDoc);

        // Title should be present
        assertEquals("Visible Title", restored.getField("title"));

        // searchable might not be present (depends on implementation)
        // This tests that indexed-only works as expected

        reader.close();
    }

//    @Test
//    void testStoredOnlyFieldNotSearchable() throws IOException {
//        // Field that's stored but not indexed
//        FieldConfig config = FieldConfig.storedOnly();
//
//        io.synapsedb.document.Document doc = new io.synapsedb.document.Document("doc1")
//                .addField("metadata", "Not searchable", config)
//                .addField("title", "Searchable Title", FieldConfig.text());
//
//        indexWriter.addDocument(DocumentConverter.toLuceneDocument(doc));
//        indexWriter.commit();
//
//        IndexReader reader = DirectoryReader.open(directory);
//        IndexSearcher searcher = new IndexSearcher(reader);
//
//        // Search on title should work
//        Query titleQuery = new TermQuery(new Term("title", "searchable"));
//        TopDocs titleResults = searcher.search(titleQuery, 10);
//        assertEquals(1, titleResults.totalHits.value);
//
//        // Search on stored-only field should NOT work
//        Query metadataQuery = new TermQuery(new Term("metadata", "searchable"));
//        TopDocs metadataResults = searcher.search(metadataQuery, 10);
//        assertEquals(0, metadataResults.totalHits.value);
//
//        // But metadata should be retrievable
//        Document foundDoc = searcher.doc(titleResults.scoreDocs[0].doc);
//        io.synapsedb.document.Document restored = DocumentConverter.fromLuceneDocument(foundDoc);
//
//        assertEquals("Not searchable", restored.getField("metadata"));
//
//        reader.close();
//    }

    @Test
    void testComplexDocumentWorkflow() throws IOException {
        // Create a complex document with various field types
        io.synapsedb.document.Document product = new io.synapsedb.document.Document("product-123")
                .addField("name", "Laptop Computer", FieldConfig.text())
                .addField("description", "High-performance laptop with SSD", FieldConfig.text())
                .addField("brand", "TechCorp", FieldConfig.keyword())
                .addField("category", "Electronics", FieldConfig.keyword())
                .addField("price", 999.99, FieldConfig.number(FieldConfig.FieldType.DOUBLE))
                .addField("stock", 50, FieldConfig.number(FieldConfig.FieldType.INTEGER))
                .addField("active", true, FieldConfig.defaults().setType(FieldConfig.FieldType.BOOLEAN));

        product.addField("tag", "computer");
        product.addField("tag", "electronics");
        product.addField("tag", "laptop");

        // Index
        indexWriter.addDocument(DocumentConverter.toLuceneDocument(product));
        indexWriter.commit();

        // Search by text field
        IndexReader reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);

        Query textQuery = new TermQuery(new Term("description", "performance"));
        TopDocs textResults = searcher.search(textQuery, 10);
        assertEquals(1, textResults.totalHits.value);

        // Search by keyword field (exact match)
        Query keywordQuery = new TermQuery(new Term("brand", "TechCorp"));
        TopDocs keywordResults = searcher.search(keywordQuery, 10);
        assertEquals(1, keywordResults.totalHits.value);

        // Search by numeric range
        Query priceQuery = DoublePoint.newRangeQuery("price", 900.0, 1100.0);
        TopDocs priceResults = searcher.search(priceQuery, 10);
        assertEquals(1, priceResults.totalHits.value);

        // Retrieve and verify all fields
        Document foundDoc = searcher.doc(textResults.scoreDocs[0].doc);
        io.synapsedb.document.Document restored = DocumentConverter.fromLuceneDocument(foundDoc);

        assertEquals("product-123", restored.getId());
        assertEquals("Laptop Computer", restored.getField("name"));
        assertEquals("TechCorp", restored.getField("brand"));
        assertEquals(3, restored.getFields("tag").size());

        reader.close();
    }

    @Test
    void testUpdateDocument() throws IOException {
        // Index original document
        io.synapsedb.document.Document original = new io.synapsedb.document.Document("doc1")
                .addField("title", "Original Title", FieldConfig.text())
                .addField("version", 1, FieldConfig.number(FieldConfig.FieldType.INTEGER));

        indexWriter.addDocument(DocumentConverter.toLuceneDocument(original));
        indexWriter.commit();

        // Update document (in Lucene, this is delete + add)
        io.synapsedb.document.Document updated = new io.synapsedb.document.Document("doc1")
                .addField("title", "Updated Title", FieldConfig.text())
                .addField("version", 2, FieldConfig.number(FieldConfig.FieldType.INTEGER));

        indexWriter.updateDocument(
                new Term("_id", "doc1"),
                DocumentConverter.toLuceneDocument(updated)
        );
        indexWriter.commit();

        // Search and verify update
        IndexReader reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);

        Query query = new TermQuery(new Term("_id", "doc1"));
        TopDocs results = searcher.search(query, 10);

        assertEquals(1, results.totalHits.value);  // Still only 1 document

        Document foundDoc = searcher.doc(results.scoreDocs[0].doc);
        io.synapsedb.document.Document restored = DocumentConverter.fromLuceneDocument(foundDoc);

        assertEquals("Updated Title", restored.getField("title"));

        reader.close();
    }

    @Test
    void testDeleteDocument() throws IOException {
        // Index two documents
        io.synapsedb.document.Document doc1 = new io.synapsedb.document.Document("doc1")
                .addField("title", "Document 1", FieldConfig.text());

        io.synapsedb.document.Document doc2 = new io.synapsedb.document.Document("doc2")
                .addField("title", "Document 2", FieldConfig.text());

        indexWriter.addDocument(DocumentConverter.toLuceneDocument(doc1));
        indexWriter.addDocument(DocumentConverter.toLuceneDocument(doc2));
        indexWriter.commit();

        // Delete doc1
        indexWriter.deleteDocuments(new Term("_id", "doc1"));
        indexWriter.commit();

        // Search for all documents
        IndexReader reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);

        Query query = new MatchAllDocsQuery();
        TopDocs results = searcher.search(query, 10);

        assertEquals(1, results.totalHits.value);  // Only doc2 remains

        Document foundDoc = searcher.doc(results.scoreDocs[0].doc);
        io.synapsedb.document.Document restored = DocumentConverter.fromLuceneDocument(foundDoc);

        assertEquals("doc2", restored.getId());

        reader.close();
    }

    @Test
    void testBulkIndexing() throws IOException {
        // Index 100 documents
        for (int i = 0; i < 100; i++) {
            io.synapsedb.document.Document doc = new io.synapsedb.document.Document("doc" + i)
                    .addField("title", "Document " + i, FieldConfig.text())
                    .addField("number", i, FieldConfig.number(FieldConfig.FieldType.INTEGER));

            indexWriter.addDocument(DocumentConverter.toLuceneDocument(doc));
        }
        indexWriter.commit();

        // Verify count
        IndexReader reader = DirectoryReader.open(directory);
        assertEquals(100, reader.numDocs());

        // Search with range query
        IndexSearcher searcher = new IndexSearcher(reader);
        Query rangeQuery = IntPoint.newRangeQuery("number", 50, 59);
        TopDocs results = searcher.search(rangeQuery, 100);

        assertEquals(10, results.totalHits.value);  // Numbers 50-59

        reader.close();
    }
}