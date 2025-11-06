package io.synapsedb.examples.search;

import io.synapsedb.SynapseDB;
import io.synapsedb.aggregation.AggregationPipeline;
import io.synapsedb.collection.Collection;
import io.synapsedb.document.Document;
import io.synapsedb.search.FullTextIndex;

import java.util.List;

/**
 * Example demonstrating full-text search and aggregation capabilities
 *
 * @author Amit Tiwari
 */
public class SearchAndAggregationExample {

    public static void main(String[] args) {
        // Create database
        SynapseDB db = new SynapseDB("myDatabase");

        // Create a collection for blog posts
        Collection blogs = db.collection("blogs");

        // Enable full-text search on title and content
        blogs.enableFullTextSearch("title", "content", "tags");

        System.out.println("=== Inserting Blog Posts ===");
        insertSampleData(blogs);
        System.out.println("Inserted " + blogs.count() + " blog posts\n");

        // Example 1: Full-text search
        System.out.println("=== Example 1: Full-Text Search ===");
        fullTextSearchExample(blogs);

        // Example 2: Multi-field search
        System.out.println("\n=== Example 2: Multi-Field Search ===");
        multiFieldSearchExample(blogs);

        // Example 3: Phrase search
        System.out.println("\n=== Example 3: Phrase Search ===");
        phraseSearchExample(blogs);

        // Example 4: Aggregations
        System.out.println("\n=== Example 4: Aggregation Pipeline ===");
        aggregationExample(blogs);

        // Example 5: Complex aggregation
        System.out.println("\n=== Example 5: Complex Aggregation ===");
        complexAggregationExample(blogs);

        // Cleanup
        db.close();
        System.out.println("\nDatabase closed successfully!");
    }

    private static void insertSampleData(Collection blogs) {
        // Blog post 1
        Document post1 = new Document("1");
        post1.addField("title", "Introduction to Java Programming");
        post1.addField("content", "Java is a powerful programming language used for building enterprise applications");
        post1.addField("author", "John Doe");
        post1.addField("category", "Programming");
        post1.addField("views", 1500);
        post1.addField("likes", 120);
        post1.addField("tags", "java programming tutorial");
        blogs.insert(post1);

        // Blog post 2
        Document post2 = new Document("2");
        post2.addField("title", "Advanced Java Design Patterns");
        post2.addField("content", "Learn about design patterns in Java including Singleton, Factory, and Observer patterns");
        post2.addField("author", "Jane Smith");
        post2.addField("category", "Programming");
        post2.addField("views", 2000);
        post2.addField("likes", 180);
        post2.addField("tags", "java design patterns advanced");
        blogs.insert(post2);

        // Blog post 3
        Document post3 = new Document("3");
        post3.addField("title", "Python for Data Science");
        post3.addField("content", "Python is the go-to language for data science with libraries like NumPy and Pandas");
        post3.addField("author", "Alice Johnson");
        post3.addField("category", "Data Science");
        post3.addField("views", 3000);
        post3.addField("likes", 250);
        post3.addField("tags", "python data-science machine-learning");
        blogs.insert(post3);

        // Blog post 4
        Document post4 = new Document("4");
        post4.addField("title", "Web Development with JavaScript");
        post4.addField("content", "JavaScript powers modern web applications with frameworks like React and Vue");
        post4.addField("author", "Bob Wilson");
        post4.addField("category", "Web Development");
        post4.addField("views", 2500);
        post4.addField("likes", 200);
        post4.addField("tags", "javascript web frontend");
        blogs.insert(post4);

        // Blog post 5
        Document post5 = new Document("5");
        post5.addField("title", "Machine Learning with Python");
        post5.addField("content", "Implement machine learning algorithms using Python and scikit-learn");
        post5.addField("author", "Alice Johnson");
        post5.addField("category", "Data Science");
        post5.addField("views", 3500);
        post5.addField("likes", 300);
        post5.addField("tags", "python machine-learning ai");
        blogs.insert(post5);
    }

    private static void fullTextSearchExample(Collection blogs) {
        System.out.println("Searching for 'Java' in titles:");
        FullTextIndex.SearchResult result = blogs.search("title", "Java", 10);

        System.out.println("Found " + result.getTotalMatches() + " matches:");
        for (FullTextIndex.ScoredDocument scored : result.getResults()) {
            Document doc = scored.getDocument();
            System.out.printf("  - %s (score: %.2f)%n",
                    doc.getField("title"), scored.getScore());
        }
    }

    private static void multiFieldSearchExample(Collection blogs) {
        System.out.println("Searching for 'Python' across title, content, and tags:");
        FullTextIndex.SearchResult result = blogs.searchMultiple(
                List.of("title", "content", "tags"),
                "Python",
                10
        );

        System.out.println("Found " + result.getTotalMatches() + " matches:");
        for (FullTextIndex.ScoredDocument scored : result.getResults()) {
            Document doc = scored.getDocument();
            System.out.printf("  - %s by %s (score: %.2f)%n",
                    doc.getField("title"),
                    doc.getField("author"),
                    scored.getScore());
        }
    }

    private static void phraseSearchExample(Collection blogs) {
        System.out.println("Searching for exact phrase 'design patterns':");
        FullTextIndex.SearchResult result = blogs.phraseSearch("content", "design patterns", 10);

        System.out.println("Found " + result.getTotalMatches() + " matches:");
        for (FullTextIndex.ScoredDocument scored : result.getResults()) {
            Document doc = scored.getDocument();
            System.out.printf("  - %s%n", doc.getField("title"));
        }
    }

    private static void aggregationExample(Collection blogs) {
        System.out.println("Grouping posts by category with statistics:");

        AggregationPipeline pipeline = new AggregationPipeline()
                .addStage(AggregationPipeline.group("category")
                        .count("postCount")
                        .sum("totalViews", "views")
                        .avg("avgLikes", "likes"));

        AggregationPipeline.AggregationResult result = blogs.aggregate(pipeline);

        for (Document doc : result.getDocuments()) {
            System.out.printf("Category: %s%n", doc.getField("_id"));
            System.out.printf("  Posts: %s%n", doc.getField("postCount"));
            System.out.printf("  Total Views: %s%n", doc.getField("totalViews"));
            System.out.printf("  Avg Likes: %.1f%n", doc.getField("avgLikes"));
            System.out.println();
        }
    }

    private static void complexAggregationExample(Collection blogs) {
        System.out.println("Finding top 3 posts by views in Programming category:");

        AggregationPipeline pipeline = new AggregationPipeline()
                .addStage(AggregationPipeline.match("category", "Programming"))
                .addStage(AggregationPipeline.sort("views", false))
                .addStage(AggregationPipeline.limit(3))
                .addStage(AggregationPipeline.project("title", "author", "views", "likes"));

        AggregationPipeline.AggregationResult result = blogs.aggregate(pipeline);

        for (Document doc : result.getDocuments()) {
            System.out.printf("  - %s by %s%n", doc.getField("title"), doc.getField("author"));
            System.out.printf("    Views: %s, Likes: %s%n",
                    doc.getField("views"), doc.getField("likes"));
        }
    }
}

