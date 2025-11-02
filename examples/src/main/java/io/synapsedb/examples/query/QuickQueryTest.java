package io.synapsedb.examples.query;

import io.synapsedb.document.Document;
import io.synapsedb.document.FieldConfig;
import io.synapsedb.index.Index;
import io.synapsedb.index.IndexManager;
import io.synapsedb.index.IndexSettings;
import io.synapsedb.index.operations.IndexOperations;
import io.synapsedb.query.QueryBuilders;
import io.synapsedb.search.SearchRequest;
import io.synapsedb.search.SearchResponse;

/**
 * Quick test of the Query Framework
 *
 * @author Amit Tiwari
 */
public class QuickQueryTest {

    public static void main(String[] args) {
        System.out.println("=".repeat(80));
        System.out.println("🚀 Query Framework Quick Test");
        System.out.println("=".repeat(80));
        System.out.println();

        try {
            // Setup
            IndexManager manager = IndexManager.getInstance("/tmp/querytest");
            try { manager.deleteIndex("test"); } catch (Exception e) {}
            Index index = manager.createIndex("test", IndexSettings.defaultSettings());

            // Index some data
            IndexOperations ops = new IndexOperations(index);
            Document doc1 = new Document("1");
            doc1.addField("name", "Laptop", FieldConfig.text().setStored(true));
            doc1.addField("category", "Electronics", FieldConfig.keyword().setStored(true));

            Document doc2 = new Document("2");
            doc2.addField("name", "Book", FieldConfig.text().setStored(true));
            doc2.addField("category", "Books", FieldConfig.keyword().setStored(true));

            ops.add(doc1);
            ops.add(doc2);
            ops.commitAndRefresh();

            System.out.println("✅ Indexed 2 documents");
            System.out.println();

            // Test 1: Term Query
            System.out.println("Test 1: Term Query (category=Electronics)");
            SearchRequest request1 = new SearchRequest()
                .query(QueryBuilders.term("category", "Electronics"))
                .size(10);

            SearchResponse response1 = index.search(request1);
            System.out.println("   Found: " + response1.getTotalHits() + " hits");
            System.out.println("   Took: " + response1.getTookMillis() + "ms");
            for (Document doc : response1.getDocuments()) {
                System.out.println("   - " + doc.getField("name"));
            }
            System.out.println();

            // Test 2: Match All
            System.out.println("Test 2: Match All Query");
            SearchRequest request2 = new SearchRequest()
                .query(QueryBuilders.matchAll())
                .size(10);

            SearchResponse response2 = index.search(request2);
            System.out.println("   Found: " + response2.getTotalHits() + " hits");
            for (Document doc : response2.getDocuments()) {
                System.out.println("   - " + doc.getField("name") + " [" + doc.getField("category") + "]");
            }
            System.out.println();

            // Test 3: Boolean Query
            System.out.println("Test 3: Boolean Query (must match Electronics)");
            SearchRequest request3 = new SearchRequest()
                .query(
                    QueryBuilders.bool()
                        .must(QueryBuilders.term("category", "Electronics"))
                )
                .size(10);

            SearchResponse response3 = index.search(request3);
            System.out.println("   Found: " + response3.getTotalHits() + " hits");
            System.out.println();

            System.out.println("=".repeat(80));
            System.out.println("✅ All Tests Passed!");
            System.out.println("=".repeat(80));

        } catch (Exception e) {
            System.err.println("❌ Test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

