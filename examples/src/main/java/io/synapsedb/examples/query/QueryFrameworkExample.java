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

import java.util.ArrayList;
import java.util.List;

/**
 * Example demonstrating the Query Framework.
 * Shows how to use the high-level query API instead of raw Lucene.
 *
 * @author Amit Tiwari
 */
public class QueryFrameworkExample {

    private static final String INDEX_NAME = "query_demo";
    private static final String DATA_PATH = "/tmp/synapsedb-query";

    public static void main(String[] args) {
        System.out.println("=".repeat(80));
        System.out.println("SynapseDB Query Framework Demo");
        System.out.println("=".repeat(80));
        System.out.println();

        try {
            // Initialize
            IndexManager manager = IndexManager.getInstance(DATA_PATH);
            try { manager.deleteIndex(INDEX_NAME); } catch (Exception e) { /* ignore */ }

            Index index = manager.createIndex(INDEX_NAME, IndexSettings.defaultSettings());
            System.out.println("✅ Index created: " + INDEX_NAME);
            System.out.println();

            // Index some sample data
            indexSampleData(index);

            // Demo 1: Simple Term Query
            demo1_TermQuery(index);

            // Demo 2: Match Query
            demo2_MatchQuery(index);

            // Demo 3: Boolean Query
            demo3_BooleanQuery(index);

            // Demo 4: Match All Query
            demo4_MatchAllQuery(index);

            System.out.println();
            System.out.println("=".repeat(80));
            System.out.println("✅ Query Framework Demo Complete!");
            System.out.println("=".repeat(80));

        } catch (Exception e) {
            System.err.println("❌ Demo failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void indexSampleData(Index index) throws Exception {
        System.out.println("📦 Indexing sample data...");

        IndexOperations ops = new IndexOperations(index);
        List<Document> products = new ArrayList<>();

        // Create 20 sample products
        String[] categories = {"Electronics", "Books", "Clothing"};
        String[] brands = {"TechCorp", "ReadMore", "StyleMax"};

        for (int i = 1; i <= 20; i++) {
            String category = categories[i % categories.length];
            String brand = brands[i % brands.length];

            Document doc = new Document("PROD-" + i);
            doc.addField("name", brand + " Product " + i, FieldConfig.text().setStored(true));
            doc.addField("description", "Great product for your needs. High quality and affordable.",
                FieldConfig.text().setStored(true));
            doc.addField("category", category, FieldConfig.keyword().setStored(true));
            doc.addField("brand", brand, FieldConfig.keyword().setStored(true));
            doc.addField("price", String.valueOf(i * 50), FieldConfig.text().setStored(true));

            products.add(doc);
        }

        ops.bulkAdd(products);
        ops.commitAndRefresh();

        System.out.println("✅ Indexed " + products.size() + " products");
        System.out.println();
    }

    private static void demo1_TermQuery(Index index) throws Exception {
        System.out.println("🔍 Demo 1: Term Query (Exact Match)");
        System.out.println("-".repeat(80));

        // OLD WAY (commented out):
        // IndexSearcher searcher = index.acquireSearcher();
        // TopDocs docs = searcher.search(new TermQuery(new Term("category", "Electronics")), 10);
        // index.releaseSearcher(searcher);

        // NEW WAY - Simple and clean!
        SearchRequest request = new SearchRequest()
            .query(QueryBuilders.term("category", "Electronics"))
            .size(5);

        SearchResponse response = index.search(request);

        System.out.println("📊 Results:");
        System.out.println("   Total hits: " + response.getTotalHits());
        System.out.println("   Returned: " + response.getReturnedHits());
        System.out.println("   Took: " + response.getTookMillis() + "ms");
        System.out.println();

        System.out.println("📄 Documents:");
        for (Document doc : response.getDocuments()) {
            System.out.println("   - " + doc.getField("name") + " | Category: " + doc.getField("category"));
        }
        System.out.println();
    }

    private static void demo2_MatchQuery(Index index) throws Exception {
        System.out.println("🔍 Demo 2: Match Query (Full-Text Search)");
        System.out.println("-".repeat(80));

        // Search for "quality affordable" in description
        SearchRequest request = new SearchRequest()
            .query(QueryBuilders.match("description", "quality affordable"))
            .size(5);

        SearchResponse response = index.search(request);

        System.out.println("📊 Results:");
        System.out.println("   Total hits: " + response.getTotalHits());
        System.out.println("   Took: " + response.getTookMillis() + "ms");
        System.out.println();

        if (response.hasErrors()) {
            System.out.println("❌ Errors: " + response.getErrors());
        }

        System.out.println("📄 Top matches:");
        for (Document doc : response.getDocuments()) {
            System.out.println("   - " + doc.getField("name"));
        }
        System.out.println();
    }

    private static void demo3_BooleanQuery(Index index) throws Exception {
        System.out.println("🔍 Demo 3: Boolean Query (Combining Conditions)");
        System.out.println("-".repeat(80));

        // Find: Electronics by TechCorp
        SearchRequest request = new SearchRequest()
            .query(
                QueryBuilders.bool()
                    .must(QueryBuilders.term("category", "Electronics"))
                    .must(QueryBuilders.term("brand", "TechCorp"))
            )
            .size(5);

        SearchResponse response = index.search(request);

        System.out.println("📊 Results for: Electronics AND TechCorp");
        System.out.println("   Total hits: " + response.getTotalHits());
        System.out.println("   Took: " + response.getTookMillis() + "ms");
        System.out.println();

        System.out.println("📄 Matching products:");
        for (Document doc : response.getDocuments()) {
            System.out.println("   - " + doc.getField("name") +
                " | " + doc.getField("category") +
                " | " + doc.getField("brand"));
        }
        System.out.println();
    }

    private static void demo4_MatchAllQuery(Index index) throws Exception {
        System.out.println("🔍 Demo 4: Match All Query");
        System.out.println("-".repeat(80));

        SearchRequest request = new SearchRequest()
            .query(QueryBuilders.matchAll())
            .size(10);

        SearchResponse response = index.search(request);

        System.out.println("📊 All documents:");
        System.out.println("   Total: " + response.getTotalHits());
        System.out.println("   Showing: " + response.getReturnedHits());
        System.out.println();
    }
}

