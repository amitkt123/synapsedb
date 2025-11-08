# SynapseDB CLI

Interactive command-line interface for SynapseDB - an in-memory document database with Lucene-powered full-text search.

## Features

### Core Features
✅ **Interactive Shell** - Tab completion, command history, multi-line input  
✅ **Collection Management** - Create, drop, list collections  
✅ **CRUD Operations** - Insert, find, update, delete documents  
✅ **Full-Text Search** - Single field, multi-field, and phrase search  
✅ **Aggregation Pipeline** - Group, count, avg, sum, min, max operations  
✅ **Data Import/Export** - Load and export JSON files  
✅ **Statistics** - Database and collection-level stats  

### Search Capabilities
- **Single Field Search**: Search in one field with Lucene scoring
- **Multi-Field Search**: Search across multiple fields simultaneously
- **Phrase Search**: Exact phrase matching
- **Configurable Indexing**: Enable search on specific fields

### Aggregation Support
- Group by field with count
- Average, Sum, Min, Max aggregations
- Flexible pipeline syntax

## Installation

### Build from Source

```bash
# Clone the repository
cd synapsedb

# Build the CLI
mvn clean package -pl synapsedb-cli -am

# The executable JAR will be created at:
# synapsedb-cli/target/synapsedb-cli-0.1.0-SNAPSHOT-jar-with-dependencies.jar
```

### Run the CLI

```bash
# Option 1: Using Maven
mvn exec:java -pl synapsedb-cli

# Option 2: Using the JAR directly
java -jar synapsedb-cli/target/synapsedb-cli-0.1.0-SNAPSHOT-jar-with-dependencies.jar

# Option 3: With custom database name
java -jar synapsedb-cli/target/synapsedb-cli-0.1.0-SNAPSHOT-jar-with-dependencies.jar -d mydb
```

## Quick Start

### Basic Workflow

```bash
# Start the CLI
$ java -jar synapsedb-cli.jar

╔═══════════════════════════════════════════════════════════╗
║              SynapseDB Interactive CLI                   ║
║                    Version 0.1.0                         ║
╚═══════════════════════════════════════════════════════════╝

Type 'help' for available commands or 'exit' to quit.

# Create and use a collection
synapsedb> use books
✓ Switched to collection: books

# Load sample data
synapsedb:books> load sample-data/books.json
Loading documents..........
✓ Loaded 50 documents from sample-data/books.json

# Enable full-text search on specific fields
synapsedb:books> index title author description
✓ Full-text search enabled on fields: title, author, description

# Search for documents
synapsedb:books> search description "machine learning"
Found 5 results in 12ms:

Score: 8.4523 | ID: abc123...
  title: Introduction to Machine Learning
  author: Jane Smith
  description: A comprehensive guide to machine learning...
---

# Multi-field search
synapsedb:books> search-multi title,description python
Searching in fields: title, description
Found 8 results in 15ms:
...

# Phrase search
synapsedb:books> phrase description "comprehensive guide"
Phrase search for: "comprehensive guide" in field: description
Found 3 results in 8ms:
...

# Aggregation
synapsedb:books> aggregate group by category
Aggregation completed in 5ms

Found 5 document(s):
ID: Programming
  _id: Programming
  count: 25
---
ID: Data Science
  _id: Data Science
  count: 15
---

# Average price by category
synapsedb:books> aggregate avg price by category
Aggregation completed in 6ms

Found 5 document(s):
ID: Programming
  _id: Programming
  average_price: 54.99
  count: 25
---

# Export data
synapsedb:books> export output/my-books.json
✓ Exported 50 documents to output/my-books.json

# Show statistics
synapsedb:books> stats
=== Database Statistics ===
Database: mydb
Collections: 1
Total Documents: 50

Current Collection: books
  Documents: 50
  Searchable Fields: 3
```

## Command Reference

### Database Operations

| Command | Description | Example |
|---------|-------------|---------|
| `show` | List all collections | `show` |
| `use <collection>` | Switch to a collection | `use products` |
| `create <collection>` | Create a new collection | `create users` |
| `drop <collection>` | Drop a collection | `drop temp` |
| `stats` | Show database statistics | `stats` |

### Document Operations

| Command | Description | Example |
|---------|-------------|---------|
| `insert <json>` | Insert a document | `insert {"name":"Laptop","price":999}` |
| `find [field=value]` | Find documents | `find category=Electronics` |
| `find` | Find all documents | `find` |
| `findById <id>` | Find document by ID | `findById abc-123` |
| `update <id> <json>` | Update a document | `update abc-123 {"price":899}` |
| `delete <id>` | Delete a document | `delete abc-123` |
| `count` | Count documents | `count` |

### Search Operations

| Command | Description | Example |
|---------|-------------|---------|
| `index <field1> <field2>` | Enable full-text search | `index title description` |
| `search <field> <query>` | Single-field search | `search title laptop` |
| `search-multi <fields> <query>` | Multi-field search | `search-multi title,desc gaming` |
| `phrase <field> <phrase>` | Exact phrase search | `phrase desc "business laptop"` |

### Aggregation

| Command | Description | Example |
|---------|-------------|---------|
| `aggregate group by <field>` | Group and count | `aggregate group by category` |
| `aggregate avg <field> by <group>` | Average aggregation | `aggregate avg price by category` |
| `aggregate sum <field> by <group>` | Sum aggregation | `aggregate sum sales by region` |
| `aggregate min <field> by <group>` | Min aggregation | `aggregate min price by category` |
| `aggregate max <field> by <group>` | Max aggregation | `aggregate max price by category` |

### Data Import/Export

| Command | Description | Example |
|---------|-------------|---------|
| `load <file.json>` | Load documents from file | `load data/books.json` |
| `export <file.json>` | Export all documents | `export backup/books.json` |

### Utility Commands

| Command | Description |
|---------|-------------|
| `clear` | Clear screen |
| `help` | Show help message |
| `exit` or `quit` | Exit the CLI |

## Example Use Cases

### 1. Product Catalog

```bash
synapsedb> use products
synapsedb:products> insert {"name":"ThinkPad X1","category":"Electronics","price":1299.99,"brand":"Lenovo"}
synapsedb:products> insert {"name":"MacBook Pro","category":"Electronics","price":2399.99,"brand":"Apple"}
synapsedb:products> insert {"name":"Desk Chair","category":"Furniture","price":299.99,"brand":"Herman Miller"}

synapsedb:products> index name brand category
synapsedb:products> search name laptop
synapsedb:products> aggregate avg price by category
synapsedb:products> find category=Electronics
```

### 2. Book Library

```bash
synapsedb> use library
synapsedb:library> load sample-data/books.json
synapsedb:library> index title author description tags
synapsedb:library> search-multi title,description "python programming"
synapsedb:library> aggregate group by category
synapsedb:library> aggregate avg price by category
synapsedb:library> export backup/library-backup.json
```

### 3. Log Analysis

```bash
synapsedb> use logs
synapsedb:logs> insert {"level":"ERROR","message":"Connection timeout","service":"api","timestamp":"2024-01-15"}
synapsedb:logs> insert {"level":"WARN","message":"Slow query detected","service":"db","timestamp":"2024-01-15"}
synapsedb:logs> index message service level
synapsedb:logs> search message timeout
synapsedb:logs> aggregate group by level
synapsedb:logs> find level=ERROR
```

## JSON Format

### Inserting Documents

Documents are inserted as JSON objects:

```json
{
  "field1": "string value",
  "field2": 123.45,
  "field3": true,
  "field4": ["array", "values"]
}
```

### Loading from File

JSON files can be either:

**Single Object:**
```json
{"name": "Product", "price": 99.99}
```

**Array of Objects:**
```json
[
  {"name": "Product 1", "price": 99.99},
  {"name": "Product 2", "price": 149.99}
]
```

## Performance Tips

1. **Enable indexing before bulk insert** - For best search performance
2. **Use specific field searches** - Faster than multi-field searches
3. **Limit result sets** - Default limit is 20 results
4. **Export data periodically** - Data is in-memory only

## Limitations

⚠️ **In-Memory Only** - All data is stored in RAM and lost on exit  
⚠️ **No Persistence** - No disk storage or crash recovery  
⚠️ **Single Node** - No distributed features  
⚠️ **RAM Limited** - Dataset size limited by available memory  

## Troubleshooting

### CLI doesn't start
- Ensure Java 11+ is installed: `java -version`
- Check if JAR file exists in target directory
- Try rebuilding: `mvn clean package -pl synapsedb-cli -am`

### Cannot load JSON file
- Verify file path is correct (relative or absolute)
- Ensure JSON is valid (use a JSON validator)
- Check file permissions

### Search returns no results
- Ensure full-text search is enabled: `index <fields>`
- Verify field names are correct
- Try simpler query terms

### Out of Memory errors
- Reduce dataset size
- Increase JVM heap: `java -Xmx4g -jar synapsedb-cli.jar`
- Export and clear collections periodically

## Development

### Project Structure

```
synapsedb-cli/
├── src/main/java/io/synapsedb/cli/
│   └── SynapseDBCLI.java          # Main CLI implementation
├── sample-data/
│   └── books.json                  # Sample dataset
├── pom.xml                         # Maven configuration
└── README.md                       # This file
```

### Dependencies

- **synapsedb-core** - Core database engine
- **JLine 3** - Terminal handling and line editing
- **Picocli** - Command-line parsing
- **Gson** - JSON processing
- **AsciiTable** - Table formatting

### Building

```bash
# Build CLI only
mvn clean package -pl synapsedb-cli -am

# Run tests
mvn test -pl synapsedb-cli

# Create executable with all dependencies
mvn clean package assembly:single -pl synapsedb-cli
```

## Contributing

Contributions welcome! Please ensure:
- Code follows existing style
- All commands have usage examples in help
- Error messages are user-friendly
- Performance is considered for large datasets

## License

See main project LICENSE file.

## Support

For issues, questions, or feature requests, please visit the main SynapseDB repository.

