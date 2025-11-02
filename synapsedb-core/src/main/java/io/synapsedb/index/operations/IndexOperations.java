package io.synapsedb.index.operations;

import io.synapsedb.document.Document;
import io.synapsedb.exception.InvalidIndexStateException;
import io.synapsedb.index.Index;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * High-level operations for index management.
 * Provides convenience methods for common index operations.
 *
 * @author Amit Tiwari
 */
public class IndexOperations {

    private final Index index;

    public IndexOperations(Index index) {
        this.index = index;
    }

    /**
     * Add a single document to the index.
     * Convenience method for adding one document at a time.
     *
     * @param document document to add
     */
    public void add(Document document) throws IOException, InvalidIndexStateException {
        index.addDocument(document);
    }

    /**
     * Bulk add documents to the index.
     * More efficient than adding one at a time.
     *
     * @param documents list of documents to add
     * @return number of documents successfully added
     */
    public int bulkAdd(List<Document> documents) throws IOException, InvalidIndexStateException {
        int successCount = 0;
        List<Exception> errors = new ArrayList<>();

        for (Document doc : documents) {
            try {
                index.addDocument(doc);
                successCount++;
            } catch (Exception e) {
                errors.add(e);
            }
        }

        // Optionally throw if any failed
        if (!errors.isEmpty() && successCount == 0) {
            throw new IOException("Failed to add any documents. First error: " + errors.get(0).getMessage());
        }

        return successCount;
    }

    /**
     * Bulk update documents.
     *
     * @param documents list of documents to update (must have IDs)
     * @return number of documents successfully updated
     */
    public int bulkUpdate(List<Document> documents) throws IOException, InvalidIndexStateException {
        int successCount = 0;

        for (Document doc : documents) {
            if (doc.getId() == null) {
                continue; // Skip documents without IDs
            }
            try {
                index.updateDocument(doc.getId(), doc);
                successCount++;
            } catch (Exception e) {
                // Log error but continue
            }
        }

        return successCount;
    }

    /**
     * Bulk delete documents by IDs.
     *
     * @param ids list of document IDs to delete
     * @return number of documents successfully deleted
     */
    public int bulkDelete(List<String> ids) throws IOException, InvalidIndexStateException {
        int successCount = 0;

        for (String id : ids) {
            try {
                index.deleteDocument(id);
                successCount++;
            } catch (Exception e) {
                // Log error but continue
            }
        }

        return successCount;
    }

    /**
     * Add documents and automatically refresh.
     * Convenient for real-time requirements.
     *
     * @param documents documents to add
     * @return number of documents added
     */
    public int addAndRefresh(List<Document> documents) throws IOException, InvalidIndexStateException {
        int count = bulkAdd(documents);
        index.refresh();
        return count;
    }

    /**
     * Add, commit, and refresh.
     * Ensures durability and searchability.
     *
     * @param documents documents to add
     * @return number of documents added
     */
    public int addCommitAndRefresh(List<Document> documents) throws IOException, InvalidIndexStateException {
        int count = bulkAdd(documents);
        index.commit();
        index.refresh();
        return count;
    }

    /**
     * Search and return count only (no documents).
     * More efficient when you only need the count.
     *
     * @param query the search query
     * @return number of matching documents
     */
    public long searchCount(Query query) throws IOException, InvalidIndexStateException {
        IndexSearcher searcher = index.acquireSearcher();
        try {
            TopDocs results = searcher.search(query, 1);
            return results.totalHits.value;
        } finally {
            index.releaseSearcher(searcher);
        }
    }

    /**
     * Check if index is empty.
     *
     * @return true if index has no documents
     */
    public boolean isEmpty() {
        return index.getStats().getTotalDocs() == 0;
    }

    /**
     * Get the number of documents in the index.
     *
     * @return document count
     */
    public long getDocumentCount() {
        return index.getStats().getTotalDocs();
    }

    /**
     * Optimize the index by forcing merge to a single segment.
     * Use with caution - can be expensive!
     */
    public void optimize() throws IOException, InvalidIndexStateException {
        index.forceMerge(1);
    }

    /**
     * Flush and refresh the index.
     * Ensures all changes are written and searchable.
     */
    public void flushAndRefresh() throws IOException, InvalidIndexStateException {
        index.flush();
        index.refresh();
    }

    /**
     * Full commit and refresh cycle.
     * Ensures durability and searchability.
     */
    public void commitAndRefresh() throws IOException, InvalidIndexStateException {
        index.commit();
        index.refresh();
    }

    /**
     * Get the underlying index.
     *
     * @return the index
     */
    public Index getIndex() {
        return index;
    }
}
