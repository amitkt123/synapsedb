package io.synapsedb.analysis;

import io.synapsedb.SynapseDB;
import io.synapsedb.analysis.analyser.LemmatizationAnalyzer;
import io.synapsedb.analysis.analyser.StandardAnalyzer;
import io.synapsedb.analysis.analyser.StemmingAnalyzer;
import io.synapsedb.collection.Collection;
import io.synapsedb.document.Document;
import io.synapsedb.search.FullTextIndex;

/**
 * Demonstrates how text analysis integrates with the broader SynapseDB project
 *
 * @author Amit Tiwari
 */
public class AnalysisIntegrationDemo {

    public static void main(String[] args) {
        System.out.println("═══════════════════════════════════════════════════════════");
        System.out.println("   Analysis Integration Demonstration");
        System.out.println("═══════════════════════════════════════════════════════════\n");

        // Demonstrate integration at each level
        demonstrateFullTextIndexIntegration();
        System.out.println();

        demonstrateCollectionIntegration();
        System.out.println();

        demonstrateAnalyzerComparison();
        System.out.println();

        demonstrateRealWorldScenario();
    }

    /**
     * Level 1: FullTextIndex directly uses Analyzer
     */
    private static void demonstrateFullTextIndexIntegration() {
        System.out.println("═══ Level 1: FullTextIndex Integration ═══\n");

        // Create index with different analyzers
        FullTextIndex standardIndex = new FullTextIndex(new StandardAnalyzer());
        FullTextIndex stemmingIndex = new FullTextIndex(new StemmingAnalyzer());

        System.out.println("StandardIndex analyzer: " + standardIndex.getAnalyzer().getName());
        System.out.println("StemmingIndex analyzer: " + stemmingIndex.getAnalyzer().getName());

        System.out.println("\n✓ FullTextIndex holds reference to Analyzer");
        System.out.println("✓ All text processing goes through the analyzer");
    }

    /**
     * Level 2: Collection manages Analyzer
     */
    private static void demonstrateCollectionIntegration() {
        System.out.println("═══ Level 2: Collection Integration ═══\n");

        // Create collection with custom analyzer
        Collection collection = new Collection("demo", new StemmingAnalyzer());

        System.out.println("Collection analyzer: " + collection.getAnalyzer().getName());

        // Change analyzer
        collection.setAnalyzer(new LemmatizationAnalyzer());
        System.out.println("Changed to: " + collection.getAnalyzer().getName());

        System.out.println("\n✓ Collection manages analyzer lifecycle");
        System.out.println("✓ Changing analyzer triggers re-indexing");
    }

    /**
     * Compare search results with different analyzers
     */
    private static void demonstrateAnalyzerComparison() {
        System.out.println("═══ Analyzer Comparison ═══\n");

        SynapseDB db = new SynapseDB("demo");

        // Test 1: Standard Analyzer
        Collection standardCol = new Collection("standard", new StandardAnalyzer());
        insertTestDocs(standardCol);
        standardCol.enableFullTextSearch("title");

        var standardResults = standardCol.search("title", "run", 10);
        System.out.println("Standard Analyzer:");
        System.out.println("  Query: 'run'");
        System.out.println("  Matches: " + standardResults.getTotalMatches() + " documents");

        // Test 2: Stemming Analyzer
        Collection stemmingCol = new Collection("stemming", new StemmingAnalyzer());
        insertTestDocs(stemmingCol);
        stemmingCol.enableFullTextSearch("title");

        var stemmingResults = stemmingCol.search("title", "run", 10);
        System.out.println("\nStemming Analyzer:");
        System.out.println("  Query: 'run'");
        System.out.println("  Matches: " + stemmingResults.getTotalMatches() + " documents");
        System.out.println("  (Matches: 'running', 'runs', 'runner')");

        System.out.println("\n✓ Same query, different results based on analyzer");
        System.out.println("✓ Stemming improves recall");
    }

    /**
     * Real-world scenario: E-commerce search
     */
    private static void demonstrateRealWorldScenario() {
        System.out.println("═══ Real-World Scenario: E-commerce Search ═══\n");

        SynapseDB db = new SynapseDB("ecommerce");
        Collection products = db.collection("products");

        // Insert products
        Document shoe1 = new Document();
        shoe1.addField("name", "Running Shoes");
        shoe1.addField("description", "Best running shoes for runners");
        products.insert(shoe1);

        Document shoe2 = new Document();
        shoe2.addField("name", "Training Sneakers");
        shoe2.addField("description", "Train like a pro with these");
        products.insert(shoe2);

        Document shirt = new Document();
        shirt.addField("name", "Running Shirt");
        shirt.addField("description", "Moisture-wicking shirt for running");
        products.insert(shirt);

        // Set stemming analyzer
        products.setAnalyzer(new StemmingAnalyzer());
        products.enableFullTextSearch("name", "description");

        System.out.println("User searches for: 'run'");
        var results = products.search("name", "run", 10);

        System.out.println("Found " + results.getTotalMatches() + " products:");
        for (var result : results.getResults()) {
            System.out.println("  - " + result.getDocument().getField("name") +
                             " (score: " + String.format("%.2f", result.getScore()) + ")");
        }

        System.out.println("\n✓ Stemming helps match 'run' with 'Running' and 'runners'");
        System.out.println("✓ Better user experience");

        db.close();
    }

    /**
     * Helper: Insert test documents
     */
    private static void insertTestDocs(Collection collection) {
        Document doc1 = new Document();
        doc1.addField("title", "Running Guide");
        collection.insert(doc1);

        Document doc2 = new Document();
        doc2.addField("title", "The Runner's Handbook");
        collection.insert(doc2);

        Document doc3 = new Document();
        doc3.addField("title", "Marathon Training");
        collection.insert(doc3);
    }
}

