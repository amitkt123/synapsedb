package io.synapsedb.core.examples.realdata;

import io.synapsedb.core.document.Document;
import io.synapsedb.core.document.FieldConfig;
import io.synapsedb.core.document.FieldType;
import io.synapsedb.core.index.Index;
import io.synapsedb.core.index.IndexManager;
import io.synapsedb.core.index.IndexSettings;
import io.synapsedb.core.index.operations.IndexOperations;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;

import java.util.*;

/**
 * Real-world data test with products, users, and blog posts.
 * Demonstrates practical usage of SynapseDB with realistic datasets.
 *
 * @author Amit Tiwari
 */
public class RealDataTest {

    private static final String DATA_PATH = "/tmp/synapsedb-realdata";
    private static final String PRODUCTS_INDEX = "products";
    private static final String USERS_INDEX = "users";
    private static final String BLOG_INDEX = "blog_posts";

    public static void main(String[] args) {
        System.out.println("=".repeat(100));
        System.out.println("🚀 SynapseDB Real Data Test");
        System.out.println("=".repeat(100));
        System.out.println();

        try {
            IndexManager manager = IndexManager.getInstance(DATA_PATH);

            // Test 1: E-commerce Products
            testProductsCatalog(manager);

            // Test 2: User Profiles
            testUserProfiles(manager);

            // Test 3: Blog Posts
            testBlogPosts(manager);

            System.out.println();
            System.out.println("=".repeat(100));
            System.out.println("✅ ALL REAL DATA TESTS PASSED!");
            System.out.println("=".repeat(100));

        } catch (Exception e) {
            System.err.println("❌ Test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Test 1: E-commerce Product Catalog
     * Realistic product data with search, filtering, and sorting
     */
    private static void testProductsCatalog(IndexManager manager) throws Exception {
        System.out.println("📦 TEST 1: E-COMMERCE PRODUCT CATALOG");
        System.out.println("-".repeat(100));

        // Clean up
        try { manager.deleteIndex(PRODUCTS_INDEX); } catch (Exception e) { /* ignore */ }

        // Create index
        Index productsIndex = manager.createIndex(PRODUCTS_INDEX, IndexSettings.defaultSettings());
        IndexOperations ops = new IndexOperations(productsIndex);

        System.out.println("Creating realistic product catalog...");

        // Create 100 realistic products
        List<Document> products = new ArrayList<>();
        String[] categories = {"Electronics", "Clothing", "Books", "Home & Garden", "Sports", "Toys"};
        String[] brands = {"TechCorp", "StyleMax", "HomeEssentials", "SportPro", "KidZone", "EliteGear"};
        Random random = new Random(42); // Fixed seed for reproducibility

        for (int i = 1; i <= 100; i++) {
            String category = categories[random.nextInt(categories.length)];
            String brand = brands[random.nextInt(brands.length)];
            double price = 9.99 + random.nextDouble() * 990.01; // $9.99 - $1000.00
            int stock = random.nextInt(1000);
            double rating = 1.0 + random.nextDouble() * 4.0; // 1.0 - 5.0

            String productName = generateProductName(category, brand, i);
            String description = generateProductDescription(productName);

            Document product = new Document("PROD-" + String.format("%05d", i));
            product.addField("name", productName, FieldConfig.builder().type(FieldType.TEXT).stored(true).tokenized(true).build());
            product.addField("description", description, FieldConfig.builder().type(FieldType.TEXT).stored(true).tokenized(true).build());
            product.addField("category", category, FieldConfig.builder().type(FieldType.KEYWORD).stored(true).tokenized(false).build());
            product.addField("brand", brand, FieldConfig.builder().type(FieldType.KEYWORD).stored(true).tokenized(false).build());
            product.addField("price", String.valueOf(price), FieldConfig.builder().type(FieldType.TEXT).stored(true).tokenized(true).build());
            product.addField("stock", String.valueOf(stock), FieldConfig.builder().type(FieldType.TEXT).stored(true).tokenized(true).build());
            product.addField("rating", String.valueOf(rating), FieldConfig.builder().type(FieldType.TEXT).stored(true).tokenized(true).build());
            product.addField("available", stock > 0 ? "true" : "false", FieldConfig.builder().type(FieldType.KEYWORD).stored(true).tokenized(false).build());

            products.add(product);
        }

        // Bulk add products
        int added = ops.bulkAdd(products);
        ops.commitAndRefresh();

        System.out.println("✅ Added " + added + " products to catalog");
        System.out.println();

        // Search Tests
        System.out.println("🔍 Search Test 1: Find all Electronics");
        searchAndPrint(productsIndex, new TermQuery(new Term("category", "Electronics")), "name", 3, "electronics products");

        System.out.println();
        System.out.println("🔍 Search Test 2: Find products by brand 'TechCorp'");
        searchAndPrint(productsIndex, new TermQuery(new Term("brand", "TechCorp")), "name", 3, "TechCorp products");

        System.out.println();
        System.out.println("🔍 Search Test 3: Text search for 'wireless'");
        searchAndPrint(productsIndex, new TermQuery(new Term("description", "wireless")), "name", 3, "products with 'wireless'");

        System.out.println();
        System.out.println("📊 Statistics:");
        System.out.println("   Total Products: " + ops.getDocumentCount());
        System.out.println("   Index Path: " + productsIndex.getIndexPath());
        System.out.println();
    }

    /**
     * Test 2: User Profiles
     * Realistic user data with search and filtering
     */
    private static void testUserProfiles(IndexManager manager) throws Exception {
        System.out.println("👥 TEST 2: USER PROFILES");
        System.out.println("-".repeat(100));

        // Clean up
        try { manager.deleteIndex(USERS_INDEX); } catch (Exception e) { /* ignore */ }

        // Create index
        Index usersIndex = manager.createIndex(USERS_INDEX, IndexSettings.defaultSettings());
        IndexOperations ops = new IndexOperations(usersIndex);

        System.out.println("Creating realistic user profiles...");

        // Create 50 realistic users
        List<Document> users = new ArrayList<>();
        String[] firstNames = {"John", "Jane", "Mike", "Sarah", "David", "Emma", "Chris", "Lisa", "Tom", "Anna"};
        String[] lastNames = {"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Martinez", "Wilson"};
        String[] cities = {"New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"};
        String[] roles = {"user", "admin", "moderator", "premium_user"};
        Random random = new Random(42);

        for (int i = 1; i <= 50; i++) {
            String firstName = firstNames[random.nextInt(firstNames.length)];
            String lastName = lastNames[random.nextInt(lastNames.length)];
            String email = firstName.toLowerCase() + "." + lastName.toLowerCase() + "@example.com";
            String city = cities[random.nextInt(cities.length)];
            String role = roles[random.nextInt(roles.length)];
            int age = 18 + random.nextInt(63); // 18-80
            boolean active = random.nextBoolean();

            Document user = new Document("USER-" + String.format("%05d", i));
            user.addField("firstName", firstName, FieldConfig.builder().type(FieldType.TEXT).stored(true).tokenized(true).build());
            user.addField("lastName", lastName, FieldConfig.builder().type(FieldType.TEXT).stored(true).tokenized(true).build());
            user.addField("email", email, FieldConfig.builder().type(FieldType.KEYWORD).stored(true).tokenized(false).build());
            user.addField("city", city, FieldConfig.builder().type(FieldType.KEYWORD).stored(true).tokenized(false).build());
            user.addField("role", role, FieldConfig.builder().type(FieldType.KEYWORD).stored(true).tokenized(false).build());
            user.addField("age", String.valueOf(age), FieldConfig.builder().type(FieldType.TEXT).stored(true).tokenized(true).build());
            user.addField("active", String.valueOf(active), FieldConfig.builder().type(FieldType.KEYWORD).stored(true).tokenized(false).build());
            user.addField("fullName", firstName + " " + lastName, FieldConfig.builder().type(FieldType.TEXT).stored(true).tokenized(true).build());

            users.add(user);
        }

        // Bulk add users
        int added = ops.bulkAdd(users);
        ops.commitAndRefresh();

        System.out.println("✅ Added " + added + " user profiles");
        System.out.println();

        // Search Tests
        System.out.println("🔍 Search Test 1: Find admins");
        searchAndPrint(usersIndex, new TermQuery(new Term("role", "admin")), "email", 3, "admin users");

        System.out.println();
        System.out.println("🔍 Search Test 2: Find users in New York");
        searchAndPrint(usersIndex, new TermQuery(new Term("city", "New York")), "fullName", 3, "users in New York");

        System.out.println();
        System.out.println("🔍 Search Test 3: Find users named 'John'");
        searchAndPrint(usersIndex, new TermQuery(new Term("firstName", "john")), "fullName", 3, "users named John");

        System.out.println();
        System.out.println("📊 Statistics:");
        System.out.println("   Total Users: " + ops.getDocumentCount());
        System.out.println("   Index Path: " + usersIndex.getIndexPath());
        System.out.println();
    }

    /**
     * Test 3: Blog Posts
     * Realistic blog content with full-text search
     */
    private static void testBlogPosts(IndexManager manager) throws Exception {
        System.out.println("📝 TEST 3: BLOG POSTS");
        System.out.println("-".repeat(100));

        // Clean up
        try { manager.deleteIndex(BLOG_INDEX); } catch (Exception e) { /* ignore */ }

        // Create index
        Index blogIndex = manager.createIndex(BLOG_INDEX, IndexSettings.defaultSettings());
        IndexOperations ops = new IndexOperations(blogIndex);

        System.out.println("Creating realistic blog posts...");

        // Create 30 realistic blog posts
        List<Document> posts = new ArrayList<>();
        String[] authors = {"Alice Cooper", "Bob Smith", "Carol Johnson", "Dave Wilson", "Eve Martinez"};
        String[] tags = {"technology", "programming", "java", "database", "search", "tutorial", "performance", "design"};
        Random random = new Random(42);

        String[][] blogData = {
            {"Getting Started with SynapseDB", "Learn how to build high-performance search applications using SynapseDB, a modern Java-based search engine."},
            {"Full-Text Search Best Practices", "Discover the best practices for implementing full-text search in your applications with real-world examples."},
            {"Optimizing Database Queries", "A comprehensive guide to optimizing database queries for better performance and scalability."},
            {"Introduction to Apache Lucene", "Understanding the core concepts of Apache Lucene and how it powers search engines."},
            {"Java Performance Tuning", "Essential tips and tricks for tuning Java applications for maximum performance."},
            {"Building Scalable Systems", "Architecture patterns and practices for building scalable distributed systems."},
            {"Search Engine Internals", "Deep dive into how search engines work under the hood, from indexing to retrieval."},
            {"Modern Java Development", "Exploring modern Java features and best practices for clean, maintainable code."},
            {"Database Design Patterns", "Common database design patterns and when to use them in your applications."},
            {"Microservices Architecture", "Building microservices with Java: patterns, practices, and pitfalls to avoid."},
            {"API Design Principles", "Creating robust and user-friendly APIs that developers will love to use."},
            {"Cloud Native Applications", "Building cloud-native applications with modern frameworks and tools."},
            {"Test-Driven Development", "How test-driven development improves code quality and developer productivity."},
            {"Continuous Integration Best Practices", "Setting up effective CI/CD pipelines for Java applications."},
            {"Docker for Java Developers", "Containerizing Java applications with Docker for consistent deployments."},
            {"Kubernetes Fundamentals", "Understanding Kubernetes core concepts and deploying Java applications."},
            {"RESTful API Development", "Building RESTful APIs with Java: design, implementation, and testing."},
            {"Security Best Practices", "Essential security practices for protecting your Java applications."},
            {"Monitoring and Observability", "Implementing effective monitoring and observability in distributed systems."},
            {"Code Review Guidelines", "Best practices for conducting effective code reviews that improve quality."},
            {"Agile Development Practices", "Applying agile methodologies to improve team productivity and delivery."},
            {"GraphQL vs REST", "Comparing GraphQL and REST: when to use each approach in your projects."},
            {"Event-Driven Architecture", "Building event-driven systems with message queues and event sourcing."},
            {"Caching Strategies", "Effective caching strategies to improve application performance and scalability."},
            {"NoSQL Database Comparison", "Comparing different NoSQL databases and choosing the right one for your needs."},
            {"Machine Learning for Developers", "Introduction to machine learning concepts for software developers."},
            {"WebSocket Communication", "Real-time communication with WebSockets in Java applications."},
            {"OAuth2 Authentication", "Implementing OAuth2 authentication and authorization in your applications."},
            {"Reactive Programming", "Understanding reactive programming paradigms and frameworks in Java."},
            {"Performance Monitoring Tools", "Essential tools for monitoring and profiling Java application performance."}
        };

        for (int i = 0; i < blogData.length; i++) {
            String author = authors[random.nextInt(authors.length)];
            String title = blogData[i][0];
            String content = blogData[i][1];

            // Add more content to make it more realistic
            content += " " + generateBlogContent(title);

            // Random tags
            List<String> postTags = new ArrayList<>();
            for (int j = 0; j < 2 + random.nextInt(3); j++) {
                postTags.add(tags[random.nextInt(tags.length)]);
            }
            String tagsStr = String.join(", ", postTags);

            int views = random.nextInt(10000);
            int likes = random.nextInt(500);
            String publishDate = "2024-" + String.format("%02d", 1 + random.nextInt(12)) + "-" + String.format("%02d", 1 + random.nextInt(28));

            Document post = new Document("POST-" + String.format("%05d", i + 1));
            post.addField("title", title, FieldConfig.builder().type(FieldType.TEXT).stored(true).tokenized(true).build());
            post.addField("content", content, FieldConfig.builder().type(FieldType.TEXT).stored(true).tokenized(true).build());
            post.addField("author", author, FieldConfig.builder().type(FieldType.KEYWORD).stored(true).tokenized(false).build());
            post.addField("tags", tagsStr, FieldConfig.builder().type(FieldType.TEXT).stored(true).tokenized(true).build());
            post.addField("views", String.valueOf(views), FieldConfig.builder().type(FieldType.TEXT).stored(true).tokenized(true).build());
            post.addField("likes", String.valueOf(likes), FieldConfig.builder().type(FieldType.TEXT).stored(true).tokenized(true).build());
            post.addField("publishDate", publishDate, FieldConfig.builder().type(FieldType.KEYWORD).stored(true).tokenized(false).build());

            posts.add(post);
        }

        // Bulk add posts
        int added = ops.bulkAdd(posts);
        ops.commitAndRefresh();

        System.out.println("✅ Added " + added + " blog posts");
        System.out.println();

        // Search Tests
        System.out.println("🔍 Search Test 1: Find posts about 'Java'");
        searchAndPrint(blogIndex, new TermQuery(new Term("content", "java")), "title", 3, "posts about Java");

        System.out.println();
        System.out.println("🔍 Search Test 2: Find posts by 'Alice Cooper'");
        searchAndPrint(blogIndex, new TermQuery(new Term("author", "Alice Cooper")), "title", 3, "posts by Alice Cooper");

        System.out.println();
        System.out.println("🔍 Search Test 3: Find posts about 'performance'");
        searchAndPrint(blogIndex, new TermQuery(new Term("content", "performance")), "title", 3, "posts about performance");

        System.out.println();
        System.out.println("🔍 Search Test 4: Find all posts");
        searchAndPrint(blogIndex, new MatchAllDocsQuery(), "title", 5, "total posts");

        System.out.println();
        System.out.println("📊 Statistics:");
        System.out.println("   Total Posts: " + ops.getDocumentCount());
        System.out.println("   Index Path: " + blogIndex.getIndexPath());
        System.out.println();
    }

    // Helper methods for generating realistic data

    private static String generateProductName(String category, String brand, int id) {
        Map<String, String[]> productTypes = new HashMap<>();
        productTypes.put("Electronics", new String[]{"Wireless Headphones", "Smart Watch", "Laptop", "Tablet", "Camera", "Speaker"});
        productTypes.put("Clothing", new String[]{"T-Shirt", "Jeans", "Jacket", "Sneakers", "Dress", "Hoodie"});
        productTypes.put("Books", new String[]{"Programming Guide", "Novel", "Cookbook", "Biography", "Science Book", "History Book"});
        productTypes.put("Home & Garden", new String[]{"Coffee Maker", "Blender", "Garden Tools", "Lamp", "Cushion", "Vase"});
        productTypes.put("Sports", new String[]{"Running Shoes", "Yoga Mat", "Dumbbell Set", "Tennis Racket", "Basketball", "Bicycle"});
        productTypes.put("Toys", new String[]{"Building Blocks", "Action Figure", "Board Game", "Puzzle", "Doll", "Remote Control Car"});

        String[] types = productTypes.getOrDefault(category, new String[]{"Product"});
        String type = types[id % types.length];
        return brand + " " + type + " Model " + id;
    }

    private static String generateProductDescription(String productName) {
        String[] adjectives = {"premium", "high-quality", "durable", "innovative", "sleek", "modern", "professional"};
        String[] features = {"latest technology", "ergonomic design", "long-lasting battery", "wireless connectivity", "easy to use"};

        Random random = new Random(productName.hashCode());
        String adj = adjectives[random.nextInt(adjectives.length)];
        String feature = features[random.nextInt(features.length)];

        return String.format("Experience the %s %s with %s. Perfect for everyday use with outstanding performance and reliability.",
            adj, productName, feature);
    }

    private static String generateBlogContent(String title) {
        return "This comprehensive article covers everything you need to know about " + title.toLowerCase() + ". " +
               "We'll explore key concepts, best practices, and real-world examples. " +
               "Whether you're a beginner or an experienced developer, you'll find valuable insights here. " +
               "The article includes code examples, diagrams, and practical tips you can apply immediately. " +
               "Join thousands of developers who have already benefited from this guide.";
    }

    /**
     * Helper method to search and print results using the Index API
     */
    private static void searchAndPrint(Index index, Query query, String fieldName, int limit, String description) throws Exception {
        IndexSearcher searcher = index.acquireSearcher();
        try {
            TopDocs topDocs = searcher.search(query, limit);
            System.out.println("   Found " + topDocs.totalHits.value + " " + description);

            for (int i = 0; i < Math.min(limit, topDocs.scoreDocs.length); i++) {
                org.apache.lucene.document.Document doc = searcher.doc(topDocs.scoreDocs[i].doc);
                String value = doc.get(fieldName);
                String id = doc.get("_id");
                System.out.println("   " + (i + 1) + ". " + value + " (ID: " + id + ")");
            }
        } finally {
            index.releaseSearcher(searcher);
        }
    }
}

