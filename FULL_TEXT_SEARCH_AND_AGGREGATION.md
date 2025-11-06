# Full-Text Search and Aggregation Features

## Overview

SynapseDB now includes advanced query features:
- **Full-Text Search** with TF-IDF scoring
- **Aggregation Pipeline** for analytical queries

## Full-Text Search

### Features
- TF-IDF (Term Frequency-Inverse Document Frequency) scoring
- Multi-field search
- Phrase search (exact phrase matching)
- Stop words filtering
- Case-insensitive search
- Pagination support

### Usage

```java
// Create database and collection
SynapseDB db = new SynapseDB("mydb");
Collection articles = db.collection("articles");

// Enable full-text search on specific fields
articles.enableFullTextSearch("title", "content", "tags");

// Insert documents
Document doc = new Document();
doc.addField("title", "Introduction to Java Programming");
doc.addField("content", "Java is a powerful programming language...");
doc.addField("tags", "java programming tutorial");
articles.insert(doc);

// Search in a single field
FullTextIndex.SearchResult result = articles.search("title", "Java", 10);
for (FullTextIndex.ScoredDocument scored : result.getResults()) {
    System.out.println(scored.getDocument().getField("title") + 
                      " (score: " + scored.getScore() + ")");
}

// Search across multiple fields
FullTextIndex.SearchResult multiResult = articles.searchMultiple(
    List.of("title", "content", "tags"),
    "Java programming",
    10
);

// Phrase search (exact match)
FullTextIndex.SearchResult phraseResult = articles.phraseSearch(
    "content", 
    "powerful programming language", 
    10
);
```

### Search Results

Search results are ranked by relevance using TF-IDF:
- **TF (Term Frequency)**: How often a term appears in a document
- **IDF (Inverse Document Frequency)**: How rare/common a term is across all documents
- Higher score = more relevant

## Aggregation Pipeline

### Features
- **Match**: Filter documents
- **Group**: Group documents and compute aggregations
  - count: Count documents in group
  - sum: Sum numeric field values
  - avg: Calculate average
  - min: Find minimum value
  - max: Find maximum value
- **Sort**: Sort documents ascending/descending
- **Limit**: Limit number of results
- **Project**: Select specific fields

### Usage

```java
Collection products = db.collection("products");

// Insert sample data
products.insert(createProduct("Laptop", "Electronics", 1000.0, 5));
products.insert(createProduct("Mouse", "Electronics", 25.0, 100));
products.insert(createProduct("Chair", "Furniture", 200.0, 20));

// Example 1: Group by category with count and average
AggregationPipeline pipeline1 = new AggregationPipeline()
    .addStage(AggregationPipeline.group("category")
        .count("totalProducts")
        .avg("avgPrice", "price")
        .sum("totalValue", "price"));

AggregationPipeline.AggregationResult result1 = products.aggregate(pipeline1);
for (Document doc : result1.getDocuments()) {
    System.out.println("Category: " + doc.getField("_id"));
    System.out.println("  Products: " + doc.getField("totalProducts"));
    System.out.println("  Avg Price: " + doc.getField("avgPrice"));
    System.out.println("  Total Value: " + doc.getField("totalValue"));
}

// Example 2: Complex pipeline - Top 5 products by price in Electronics
AggregationPipeline pipeline2 = new AggregationPipeline()
    .addStage(AggregationPipeline.match("category", "Electronics"))
    .addStage(AggregationPipeline.sort("price", false)) // descending
    .addStage(AggregationPipeline.limit(5))
    .addStage(AggregationPipeline.project("name", "price"));

AggregationPipeline.AggregationResult result2 = products.aggregate(pipeline2);
```

## Complete Example

```java
public class FullExample {
    public static void main(String[] args) {
        // Create database
        SynapseDB db = new SynapseDB("blogDB");
        Collection blogs = db.collection("posts");
        
        // Enable search
        blogs.enableFullTextSearch("title", "content");
        
        // Insert blog posts
        Document post1 = new Document();
        post1.addField("title", "Getting Started with Java");
        post1.addField("content", "Java is an object-oriented language...");
        post1.addField("author", "John Doe");
        post1.addField("views", 1500);
        post1.addField("category", "Programming");
        blogs.insert(post1);
        
        Document post2 = new Document();
        post2.addField("title", "Python for Beginners");
        post2.addField("content", "Python is easy to learn...");
        post2.addField("author", "Jane Smith");
        post2.addField("views", 2000);
        post2.addField("category", "Programming");
        blogs.insert(post2);
        
        // Search for Java-related posts
        System.out.println("=== Search Results ===");
        FullTextIndex.SearchResult searchResult = 
            blogs.search("title", "Java", 10);
        
        for (FullTextIndex.ScoredDocument scored : searchResult.getResults()) {
            System.out.printf("%s (relevance: %.2f)%n",
                scored.getDocument().getField("title"),
                scored.getScore());
        }
        
        // Aggregate: Most popular authors
        System.out.println("\n=== Top Authors by Views ===");
        AggregationPipeline pipeline = new AggregationPipeline()
            .addStage(AggregationPipeline.group("author")
                .sum("totalViews", "views")
                .count("postCount"))
            .addStage(AggregationPipeline.sort("totalViews", false))
            .addStage(AggregationPipeline.limit(10));
        
        AggregationPipeline.AggregationResult aggResult = 
            blogs.aggregate(pipeline);
        
        for (Document doc : aggResult.getDocuments()) {
            System.out.printf("%s: %s views (%s posts)%n",
                doc.getField("_id"),
                doc.getField("totalViews"),
                doc.getField("postCount"));
        }
        
        db.close();
    }
}
```

## Performance Considerations

### Full-Text Search
- Indexing happens in-memory
- Suitable for moderate-sized datasets (< 1M documents)
- For larger datasets, consider external search engines like Elasticsearch

### Aggregations
- Operates on entire collection in memory
- Use `match` stage early in pipeline to filter data
- Best for analytical queries on moderate datasets

## API Reference

### Collection Methods

```java
// Enable full-text search
void enableFullTextSearch(String... fields)

// Search single field
SearchResult search(String field, String query)
SearchResult search(String field, String query, int limit)

// Search multiple fields
SearchResult searchMultiple(List<String> fields, String query, int limit)

// Phrase search
SearchResult phraseSearch(String field, String phrase, int limit)

// Execute aggregation
AggregationResult aggregate(AggregationPipeline pipeline)
```

### AggregationPipeline Static Methods

```java
// Create stages
MatchStage match(String field, Object value)
GroupStage group(String groupByField)
SortStage sort(String field, boolean ascending)
LimitStage limit(int count)
ProjectStage project(String... fields)
```

### GroupStage Methods

```java
GroupStage count(String outputField)
GroupStage sum(String outputField, String inputField)
GroupStage avg(String outputField, String inputField)
GroupStage min(String outputField, String inputField)
GroupStage max(String outputField, String inputField)
```

## Test Coverage

- 10 full-text search tests
- 14 aggregation pipeline tests
- 13 collection integration tests

Total: **37 new tests** covering all advanced query features

## Future Enhancements

Potential future improvements:
- Fuzzy matching with edit distance
- Regular expression search
- Geo-spatial queries
- Text analysis with stemming/lemmatization
- Faceted search
- More aggregation operators (unwind, lookup)
- Index persistence to disk

