package io.synapsedb;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;

import java.io.IOException;

public class HelloSynapse {
    public static void main(String[] args) throws Exception {
        Directory directory = new ByteBuffersDirectory();
        StandardAnalyzer analyser = new StandardAnalyzer();

        indexDocument(directory, analyser);

        searchIndex(directory,analyser, "java");

        directory.close();
        System.out.println("Synapse DB demo complete===");

    }

    //indexing the file
    private static void indexDocument(Directory directory, StandardAnalyzer analyser) throws IOException {
        System.out.println("Indexing Documents");
        IndexWriterConfig config = new IndexWriterConfig(analyser);
        IndexWriter writer = new IndexWriter(directory,config);
        Document doc1 = new Document();
        doc1.add(new TextField("title", "Java Programming", Field.Store.YES));
        doc1.add(new TextField("content",
                "Java is a popular object-oriented programming language. " +
                        "It runs on the JVM and is used for enterprise applications.",
                Field.Store.YES));
        doc1.add(new Field());
        writer.addDocument(doc1);
        System.out.println("✓ Indexed: Java Programming");

        // Document 2: Python
        Document doc2 = new Document();
        doc2.add(new TextField("title", "Python Guide", Field.Store.YES));
        doc2.add(new TextField("content",
                "Python is a high-level programming language known for simplicity. " +
                        "It's widely used in data science and machine learning.",
                Field.Store.YES));
        writer.addDocument(doc2);
        System.out.println("✓ Indexed: Python Guide");

        // Document 3: Lucene
        Document doc3 = new Document();
        doc3.add(new TextField("title", "Apache Lucene Tutorial", Field.Store.YES));
        doc3.add(new TextField("content",
                "Lucene is a powerful search library written in Java. " +
                        "It provides full-text indexing and search capabilities.",
                Field.Store.YES));
        writer.addDocument(doc3);
        System.out.println("✓ Indexed: Apache Lucene Tutorial");

        // Important: Close the writer to flush changes
        writer.close();

        System.out.println("\n All documents indexed successfully!\n");

    }

    private static void searchIndex(Directory directory, StandardAnalyzer analyser, String searchTerm) throws Exception{
        // Open the index for reading
        // Open the index for reading
        DirectoryReader reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);

        // Parse the query
        QueryParser parser = new QueryParser("content", analyser);
        Query query = parser.parse(searchTerm);

        System.out.println("Query: " + query.toString() + "\n");

        // Execute search (get top 10 results)
        TopDocs results = searcher.search(query, 10);

        System.out.println("Found " + results.totalHits + " matching documents:\n");
        System.out.println("─────────────────────────────────────────────────");

        // Display results
        for (ScoreDoc scoreDoc : results.scoreDocs) {
            Document doc = searcher.doc(scoreDoc.doc);

            System.out.println("📄 Document #" + scoreDoc.doc);
            System.out.println("   Score: " + scoreDoc.score);
            System.out.println("   Title: " + doc.get("title"));
            System.out.println("   Content: " + doc.get("content"));
            System.out.println("─────────────────────────────────────────────────");
        }

        reader.close();
    }
}


























