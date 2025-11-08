# SynapseDB

> Connect your data at the speed of thought

An in-memory document database with Lucene-powered full-text search and advanced text analysis capabilities. Built from scratch to deeply understand search engines and document databases.

## 🚀 Vision

Just as synapses enable rapid communication in the brain, SynapseDB enables rapid discovery across your data.This project aims to build a fast text search engine focused at analytics.Built on the top of Apache Lucene and heavily inspired by ElasticSearch, but aims to expand the capabilities further to make it an AI native db. 

## ✨ Key Features

### 🔍 Full-Text Search
- **Lucene-powered indexing** - Industry-standard inverted index
- **TF-IDF scoring** - Relevance-based ranking
- **Multi-field search** - Search across multiple fields simultaneously  
- **Phrase search** - Exact phrase matching
- **Wildcard support** - Pattern-based searching

### 📝 Advanced Text Analysis
- **Multiple analyzers** - Standard, Stemming, Lemmatization
- **Porter Stemmer** - Classic stemming algorithm (running → run)
- **Lemmatization** - Dictionary-based reduction (children → child, better → good)
- **Stop words filtering** - Remove common words
- **Pluggable architecture** - Easy to add custom analyzers

### 📊 Aggregation Framework
- **Group by** - Aggregate documents by field values
- **Statistical operations** - Count, Sum, Average, Min, Max
- **Pipeline processing** - Chain multiple aggregation stages
- **Real-time analytics** - Instant results

### 💾 Document Storage
- **Schema-less JSON** - No predefined schema required
- **CRUD operations** - Full create, read, update, delete support
- **Collections** - Organize documents into collections
- **Auto-ID generation** - Automatic UUID assignment
- **Flexible fields** - Dynamic field addition

### 🖥️ Interactive CLI
- **20+ commands** - Complete database operations
- **Tab completion** - Auto-complete commands
- **Command history** - Navigate with UP/DOWN arrows
- **JSON import/export** - Load and save data
- **Built-in help** - Comprehensive command documentation

## 📋 Current Status

**Version:** 0.1.0-SNAPSHOT  
**Status:** Core features complete, production-ready for single-node use

### ✅ Completed Features

#### Phase 1: Core Indexing & Search ✅
- [x] Project initialization & Maven structure
- [x] Lucene integration (8.11.4)
- [x] Document CRUD operations
- [x] Full-text search with TF-IDF scoring
- [x] Field mapping and indexing
- [x] Index lifecycle management

#### Phase 2: Advanced Queries & Aggregations ✅
- [x] Query framework with builders
- [x] Single and multi-field search
- [x] Phrase search
- [x] Aggregation pipeline (group, sum, avg, min, max)
- [x] Match, Range, Boolean query support

#### Phase 2.5: Text Analysis ✅ (NEW!)
- [x] Analyzer framework
- [x] Standard Analyzer (tokenization + lowercase)
- [x] Stemming Analyzer (Porter stemmer)
- [x] Lemmatization Analyzer (dictionary-based)
- [x] Tokenizers and filters
- [x] Runtime analyzer switching

#### Phase 3: CLI & User Interface ✅
- [x] Interactive CLI with JLine
- [x] 20+ commands (use, find, search, aggregate, etc.)
- [x] Tab completion and history
- [x] JSON import/export
- [x] Statistics and monitoring

### 🔄 In Progress
- Transaction logging (partial)
- Disk persistence layer

### 📅 Planned Features
- REST API (Spring Boot)
- Distributed clustering
- Backup/restore
- Authentication & authorization
- Monitoring dashboard

## 🏗️ Architecture

```
synapsedb/
├── synapsedb-core/          → Core database engine
│   ├── collection/          → Document collections
│   ├── document/            → Document model
│   ├── search/              → Full-text search (Lucene)
│   ├── analysis/            → Text analyzers 
│   │   ├── analyser/        → Standard, Stemming, Lemmatization
│   │   ├── tokeniser/       → Text tokenization
│   │   ├── filter/          → Token filters
│   │   └── stemmer/         → Porter, Simple, Lemmatizer
│   ├── aggregation/         → Aggregation pipeline
│   ├── query/               → Query framework
│   └── index/               → Index management
├── synapsedb-cli/           → Interactive command-line interface
│   └── sample-data/         → Example datasets (books.json)
├── examples/                → Code examples
└── docs/                    → Documentation
```

## 🛠️ Tech Stack

- **Java**: 17
- **Apache Lucene**: 8.11.4 (Search engine foundation)
- **Build Tool**: Maven 3.x
- **CLI**: JLine 3, Picocli
- **JSON**: Gson
- **Testing**: JUnit 5

## 📦 Getting Started

### Prerequisites

- Java 11 or higher
- Maven 3.6 or higher

### Build the Project
```bash
# Clone the repository
git clone https://github.com/amitkt123/synapsedb.git
cd synapsedb

# Build all modules
mvn clean install

# Run tests
mvn test
```

### Start the CLI
```bash
# Run the interactive CLI
java -jar synapsedb-cli/target/synapsedb-cli-0.1.0-SNAPSHOT-jar-with-dependencies.jar

# Or use the convenience script
./synapsedb-cli/run.sh
```

### Quick Start Example

```bash
# In the CLI
synapsedb> use books
✓ Switched to collection: books

# Load sample data
synapsedb:books> load synapsedb-cli/sample-data/books.json
Loading documents..........
✓ Loaded 50 documents

# Enable stemming for better search
synapsedb:books> analyzer stemming
✓ Analyzer set to: stemming

# Index fields for full-text search
synapsedb:books> index title author description
✓ Full-text search enabled on fields: title, author, description

# Search for books (finds "running", "runs", "runner", etc.)
synapsedb:books> search title run
Found 5 results in 12ms:
Score: 8.4523 | ID: abc-123
  title: Introduction to Java Programming
  ...

# Multi-field search
synapsedb:books> search-multi title,description "machine learning"
Found 8 results in 15ms:
...

# Aggregation
synapsedb:books> aggregate avg price by category
Aggregation completed in 6ms
...

# Export data
synapsedb:books> export my-books.json
✓ Exported 50 documents
```

## 💻 Programmatic Usage

```java
// Initialize database
SynapseDB db = new SynapseDB("mydb");
Collection collection = db.collection("articles");

// Insert document
Document doc = new Document();
doc.addField("title", "Introduction to Search Engines");
doc.addField("content", "Learn about inverted indices and TF-IDF scoring");
doc.addField("category", "Technology");
collection.insert(doc);

// Set text analyzer for better search
collection.setAnalyzer(new StemmingAnalyzer());

// Enable full-text search
collection.enableFullTextSearch("title", "content");

// Search
FullTextIndex.SearchResult result = collection.search("content", "search engines", 10);
System.out.println("Found " + result.getTotalMatches() + " results");

for (FullTextIndex.ScoredDocument scored : result.getResults()) {
    System.out.println("Score: " + scored.getScore());
    System.out.println("Title: " + scored.getDocument().getField("title"));
}

// Aggregation
AggregationPipeline pipeline = new AggregationPipeline();
pipeline.addStage(AggregationPipeline.group("category").count("count"));
AggregationPipeline.AggregationResult aggResult = collection.aggregate(pipeline);

// Cleanup
db.close();
```

## 🎯 Text Analysis Examples

### Stemming (Porter Algorithm)
```bash
# Document: "Children were running quickly"
synapsedb> analyzer stemming
synapsedb> index text
synapsedb> search text run
# Matches: "running" → "run" ✓
```

### Lemmatization (Dictionary-based)
```bash
# Document: "Children were running quickly"
synapsedb> analyzer lemmatization
synapsedb> search text child
# Matches: "children" → "child" ✓
synapsedb> search text run
# Matches: "running" → "run" ✓
```

## 📚 Documentation

- [Feature Outline](FEATURE_OUTLINE.md) - Complete feature list
- [CLI Demo Guide](CLI_DEMO_GUIDE.md) - Step-by-step demo script
- [Architecture Overview](docs/architecture.md) - System design
- [CLI Usage](synapsedb-cli/USAGE.md) - CLI command reference

## 🧪 Testing

```bash
# Run all tests
mvn test

# Run specific test
mvn test -Dtest=TextAnalysisTest

# Run with coverage
mvn clean test jacoco:report

# Test the CLI
./test-fulltext-search.sh
```

## 📊 Performance Characteristics

### Strengths
- ✅ **Fast in-memory operations** - No disk I/O overhead
- ✅ **Efficient indexing** - Lucene inverted index
- ✅ **Sub-millisecond searches** - For small to medium datasets
- ✅ **Real-time aggregations** - Instant analytics

### Current Limitations
- ⚠️ **In-memory only** - Dataset size limited by RAM
- ⚠️ **Single-node** - No distribution (yet)
- ⚠️ **No persistence** - Data lost on restart (transaction log partial)

## 🎓 Use Cases

### Perfect For:
1. **Prototyping** - Rapid POC development
2. **Testing** - Test search features before production
3. **Learning** - Understand search engines and text analysis
4. **Development** - Local development environment
5. **Small datasets** - Up to 100K documents
6. **Text-heavy applications** - Content search, document management

### Examples:
- Content management systems
- E-commerce product search
- Document management
- Log analysis
- Book libraries
- Knowledge bases

## 🌟 What Makes SynapseDB Unique

1. **Built from Scratch** - Deep understanding of every component
2. **Advanced Text Analysis** - Stemming and lemmatization built-in
3. **Interactive CLI** - No coding required to use it
4. **MongoDB-like API** - Familiar document-oriented interface
5. **Lucene Foundation** - Industry-standard search technology
6. **Educational Focus** - Clear, well-documented code

## 📈 Project Statistics (evolving)

- **Total Classes**: ~70
- **Lines of Code**: 8,000+
- **Test Coverage**: 80%
- **Modules**: 3 (core, cli, examples)
- **Commands**: 20+ in CLI
- **Analyzers**: 3 (Standard, Stemming, Lemmatization)

## 🔄 Distribution

```bash
# Build distribution packages
./build-distribution.sh

# Creates:
# - dist/synapsedb-standalone-0.1.0.tar.gz (Mac/Linux)
# - dist/synapsedb-standalone-0.1.0.zip (Windows)
# - dist/synapsedb-docker-0.1.0.tar.gz (Docker image)

# Docker
docker build -t synapsedb:0.1.0 .
docker run -it --rm synapsedb:0.1.0
```

## 🤝 Contributing

This is an ongoing project built to become a AI native search and analytics engine. Contributions, suggestions, and feedback are welcome!

### Areas for Contribution:
- Additional language analyzers (Spanish, French, etc.)
- Performance optimizations
- Disk persistence implementation
- REST API layer
- Additional aggregation operations
- Documentation improvements

## 📄 License

Apache License 2.0

See [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **Elasticsearch** - Architectural inspiration
- **Apache Lucene** - Search engine foundation
- **MongoDB** - Document model inspiration
- **Porter Stemmer** - Classic stemming algorithm (1980)
- Open-source community

## 📞 Contact

**Amit Tiwari**  
Email: amit.tiwari912@gmail.com  
GitHub: [@amitkt123](https://github.com/amitkt123)

For questions, discussions, or feedback about SynapseDB architecture and implementation.

---

⭐ **Star this repo** if you find it useful!

Built with ❤️ and Java | Search • Index • Analyze

