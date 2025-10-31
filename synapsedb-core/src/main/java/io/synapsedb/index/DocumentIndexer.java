package io.synapsedb.index;


import io.synapsedb.document.mapper.DocumentConverter;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;

/**
 * Utility class for indexing documents asynchronously with batching.
 * @author Amit Tiwari
 */
public class DocumentIndexer {
    private final IndexWriter indexWriter;

    public DocumentIndexer(IndexWriter indexWriter) {
        this.indexWriter = indexWriter;
    }

    /**
     * Index a SynapseDoc
     */
    public void addDocument(io.synapsedb.document.Document synapseDoc) throws Exception {
        if (!synapseDoc.isValid()) {
            throw new IllegalArgumentException("Invalid document: " +
                    synapseDoc.getValidationErrors());
        }

        Document luceneDoc = DocumentConverter.toLuceneDocument(synapseDoc);
        indexWriter.addDocument(luceneDoc);
    }

    /**
     * Update a SynapseDoc (delete old, add new)
     */
    public void updateDocument(io.synapsedb.document.Document synapseDoc) throws Exception {
        // Delete by ID
        indexWriter.deleteDocuments(
                new Term("_id", synapseDoc.getId())
        );
        // Add updated version
        addDocument(synapseDoc);
    }

    /**
     * Delete a SynapseDoc by ID
     */
    public void deleteDocument(String id) throws Exception {
        indexWriter.deleteDocuments(
                new Term("_id", id)
        );
    }

    public void commit() throws Exception {
        indexWriter.commit();
    }

    public void close() throws Exception {
        indexWriter.close();
    }
}
