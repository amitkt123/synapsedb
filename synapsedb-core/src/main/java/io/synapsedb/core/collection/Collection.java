package io.synapsedb.core.collection;

import io.synapsedb.core.aggregation.AggregationPipeline;
import io.synapsedb.core.analysis.Analyzer;
import io.synapsedb.core.analysis.analyser.StandardAnalyzer;
import io.synapsedb.core.document.Document;
import io.synapsedb.core.search.FullTextIndex;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Collection represents a logical grouping of documents with indexing and query capabilities
 *
 * @author Amit Tiwari
 */
public class Collection {
    private final String name;
    private final Map<String, Document> documents;
    private final FullTextIndex fullTextIndex;
    private final Set<String> searchableFields;

    public Collection(String name) {
        this(name, new StandardAnalyzer());
    }

    public Collection(String name, Analyzer analyzer) {
        this.name = name;
        this.documents = new ConcurrentHashMap<>();
        this.fullTextIndex = new FullTextIndex(analyzer);
        this.searchableFields = new HashSet<>();
    }

    /**
     * Set the analyzer for text analysis
     */
    public void setAnalyzer(Analyzer analyzer) {
        fullTextIndex.setAnalyzer(analyzer);
        // Re-index all documents with new analyzer
        if (!searchableFields.isEmpty()) {
            reindexAllDocuments();
        }
    }

    /**
     * Get current analyzer
     */
    public Analyzer getAnalyzer() {
        return fullTextIndex.getAnalyzer();
    }

    /**
     * Re-index all documents (called when analyzer changes)
     */
    private void reindexAllDocuments() {
        for (Document doc : documents.values()) {
            fullTextIndex.indexDocument(doc, new ArrayList<>(searchableFields));
        }
    }

    /**
     * Enable full-text search on specific fields
     */
    public void enableFullTextSearch(String... fields) {
        searchableFields.addAll(Arrays.asList(fields));
        // Re-index all existing documents
        for (Document doc : documents.values()) {
            fullTextIndex.indexDocument(doc, new ArrayList<>(searchableFields));
        }
    }

    /**
     * Insert a document into the collection
     */
    public void insert(Document document) {
        if (document.getId() == null) {
            document.setId(UUID.randomUUID().toString());
        }
        documents.put(document.getId(), document);

        // Index for full-text search if enabled
        if (!searchableFields.isEmpty()) {
            fullTextIndex.indexDocument(document, new ArrayList<>(searchableFields));
        }
    }

    /**
     * Insert multiple documents
     */
    public void insertMany(List<Document> docs) {
        for (Document doc : docs) {
            insert(doc);
        }
    }

    /**
     * Find document by ID
     */
    public Document findById(String id) {
        return documents.get(id);
    }

    /**
     * Find all documents
     */
    public List<Document> findAll() {
        return new ArrayList<>(documents.values());
    }

    /**
     * Find documents matching a filter
     */
    public List<Document> find(String field, Object value) {
        List<Document> results = new ArrayList<>();
        for (Document doc : documents.values()) {
            Object fieldValue = doc.getField(field);
            if (fieldValue != null && fieldValue.equals(value)) {
                results.add(doc);
            }
        }
        return results;
    }

    /**
     * Update a document
     */
    public boolean update(String id, Map<String, Object> updates) {
        Document doc = documents.get(id);
        if (doc == null) {
            return false;
        }

        // Remove from index before updating
        if (!searchableFields.isEmpty()) {
            fullTextIndex.removeDocument(id);
        }

        // Apply updates - use setField to replace values
        for (Map.Entry<String, Object> entry : updates.entrySet()) {
            doc.setField(entry.getKey(), entry.getValue());
        }

        // Re-index for full-text search
        if (!searchableFields.isEmpty()) {
            fullTextIndex.indexDocument(doc, new ArrayList<>(searchableFields));
        }

        return true;
    }

    /**
     * Delete a document
     */
    public boolean delete(String id) {
        Document removed = documents.remove(id);
        if (removed != null) {
            fullTextIndex.removeDocument(id);
            return true;
        }
        return false;
    }

    /**
     * Count documents in collection
     */
    public long count() {
        return documents.size();
    }

    /**
     * Full-text search in a single field
     */
    public FullTextIndex.SearchResult search(String field, String query) {
        return search(field, query, 10);
    }

    /**
     * Full-text search with limit
     */
    public FullTextIndex.SearchResult search(String field, String query, int limit) {
        return fullTextIndex.search(field, query, new FullTextIndex.SearchOptions(0, limit));
    }

    /**
     * Full-text search across multiple fields
     */
    public FullTextIndex.SearchResult searchMultiple(List<String> fields, String query, int limit) {
        return fullTextIndex.searchMultipleFields(fields, query, new FullTextIndex.SearchOptions(0, limit));
    }

    /**
     * Phrase search - exact phrase matching
     */
    public FullTextIndex.SearchResult phraseSearch(String field, String phrase, int limit) {
        return fullTextIndex.phraseSearch(field, phrase, new FullTextIndex.SearchOptions(0, limit));
    }

    /**
     * Execute an aggregation pipeline
     */
    public AggregationPipeline.AggregationResult aggregate(AggregationPipeline pipeline) {
        return pipeline.execute(new ArrayList<>(documents.values()));
    }

    /**
     * Get collection name
     */
    public String getName() {
        return name;
    }

    /**
     * Get collection statistics
     */
    public CollectionStats getStats() {
        return new CollectionStats(name, documents.size(), searchableFields.size());
    }

    /**
     * Collection statistics
     */
    public static class CollectionStats {
        private final String name;
        private final long documentCount;
        private final int searchableFieldsCount;

        public CollectionStats(String name, long documentCount, int searchableFieldsCount) {
            this.name = name;
            this.documentCount = documentCount;
            this.searchableFieldsCount = searchableFieldsCount;
        }

        public String getName() {
            return name;
        }

        public long getDocumentCount() {
            return documentCount;
        }

        public int getSearchableFieldsCount() {
            return searchableFieldsCount;
        }

        @Override
        public String toString() {
            return String.format("CollectionStats{name='%s', documents=%d, searchableFields=%d}",
                    name, documentCount, searchableFieldsCount);
        }
    }
}

