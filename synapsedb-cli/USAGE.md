# How to Use SynapseDB CLI

## Starting the CLI

### Method 1: Using the JAR directly
```bash
cd /Users/amittiwari/Desktop/synapsedb
java -jar synapsedb-cli/target/synapsedb-cli-0.1.0-SNAPSHOT-jar-with-dependencies.jar
```

### Method 2: Using the run script (easier)
```bash
cd /Users/amittiwari/Desktop/synapsedb
./synapsedb-cli/run.sh
```

### Method 3: With custom database name
```bash
java -jar synapsedb-cli/target/synapsedb-cli-0.1.0-SNAPSHOT-jar-with-dependencies.jar -d mydb
```

---

## Step-by-Step Tutorial

### Step 1: Start the CLI
```bash
java -jar synapsedb-cli/target/synapsedb-cli-0.1.0-SNAPSHOT-jar-with-dependencies.jar
```

You'll see:
```
╔═══════════════════════════════════════════════════════════╗
║              SynapseDB Interactive CLI                   ║
║                    Version 0.1.0                         ║
╚═══════════════════════════════════════════════════════════╝

Type 'help' for available commands or 'exit' to quit.

✓ Connected to database: mydb
synapsedb> 
```

### Step 2: Get Help
```
synapsedb> help
```

This shows all available commands.

### Step 3: Create/Use a Collection
```
synapsedb> use books
✓ Switched to collection: books
synapsedb:books> 
```

### Step 4: Load Sample Data
```
synapsedb:books> load synapsedb-cli/sample-data/books.json
Loading documents..........
✓ Loaded 50 documents from synapsedb-cli/sample-data/books.json
```

### Step 5: View Your Data
```
# Count documents
synapsedb:books> count
Document count: 50

# See all documents
synapsedb:books> find

# Find specific documents
synapsedb:books> find category=Programming
```

### Step 6: Enable Full-Text Search
```
synapsedb:books> index title author description
✓ Full-text search enabled on fields: title, author, description
```

### Step 7: Search Your Data
```
# Search in one field
synapsedb:books> search title java

# Search in multiple fields
synapsedb:books> search-multi title,description python

# Exact phrase search
synapsedb:books> phrase description "machine learning"
```

### Step 8: Run Analytics
```
# Group by category and count
synapsedb:books> aggregate group by category

# Calculate average price by category
synapsedb:books> aggregate avg price by category

# Sum total by field
synapsedb:books> aggregate sum pages by category
```

### Step 9: CRUD Operations
```
# Insert new document
synapsedb:books> insert {"title":"My New Book","author":"Me","price":19.99}

# Find by ID (use the ID returned from insert)
synapsedb:books> findById <the-id-here>

# Update document
synapsedb:books> update <the-id-here> {"price":14.99}

# Delete document
synapsedb:books> delete <the-id-here>
```

### Step 10: Export Your Data
```
synapsedb:books> export my-books-backup.json
✓ Exported 50 documents to my-books-backup.json
```

### Step 11: View Statistics
```
synapsedb:books> stats
```

### Step 12: Exit
```
synapsedb:books> exit
Goodbye!
```

---

## All Available Commands

### Database Operations
- `show` - List all collections
- `use <name>` - Switch to/create a collection
- `create <name>` - Create a collection
- `drop <name>` - Drop a collection
- `stats` - Show database statistics

### Document Operations
- `insert <json>` - Insert a document
- `find` - Find all documents
- `find <field>=<value>` - Find by field
- `findById <id>` - Find by document ID
- `update <id> <json>` - Update a document
- `delete <id>` - Delete a document
- `count` - Count documents

### Search Operations
- `index <field1> <field2> ...` - Enable full-text search
- `search <field> <query>` - Search in one field
- `search-multi <field1,field2> <query>` - Search multiple fields
- `phrase <field> <phrase>` - Exact phrase search

### Aggregation
- `aggregate group by <field>` - Group and count
- `aggregate avg <field> by <group>` - Calculate average
- `aggregate sum <field> by <group>` - Calculate sum
- `aggregate min <field> by <group>` - Find minimum
- `aggregate max <field> by <group>` - Find maximum

### Data Import/Export
- `load <file.json>` - Load documents from JSON file
- `export <file.json>` - Export documents to JSON file

### Utility
- `clear` - Clear screen
- `help` - Show help
- `exit` or `quit` - Exit CLI

---

## Real-World Examples

### Example 1: Product Catalog
```bash
# Start CLI
java -jar synapsedb-cli/target/synapsedb-cli-0.1.0-SNAPSHOT-jar-with-dependencies.jar

# Create products collection
synapsedb> use products

# Insert products
synapsedb:products> insert {"name":"Laptop","brand":"Dell","price":999,"category":"Electronics"}
synapsedb:products> insert {"name":"Mouse","brand":"Logitech","price":29,"category":"Electronics"}
synapsedb:products> insert {"name":"Keyboard","brand":"Corsair","price":79,"category":"Electronics"}

# Enable search
synapsedb:products> index name brand category

# Search products
synapsedb:products> search name laptop

# Group by category
synapsedb:products> aggregate group by category

# Get average price
synapsedb:products> aggregate avg price by brand
```

### Example 2: Working with Books
```bash
# Use books collection
synapsedb> use books

# Load sample data
synapsedb:books> load synapsedb-cli/sample-data/books.json

# Enable search on multiple fields
synapsedb:books> index title author description category

# Find programming books
synapsedb:books> find category=Programming

# Search for Python books
synapsedb:books> search-multi title,description python

# Find books with "comprehensive guide" phrase
synapsedb:books> phrase description "comprehensive guide"

# See which category has most books
synapsedb:books> aggregate group by category

# Get average price by category
synapsedb:books> aggregate avg price by category

# Export to backup
synapsedb:books> export books-backup.json
```

### Example 3: Testing Search Features
```bash
# Create test collection
synapsedb> use test

# Insert test documents
synapsedb:test> insert {"title":"Java Programming","content":"Learn Java basics and advanced concepts"}
synapsedb:test> insert {"title":"Python Guide","content":"Python programming from scratch"}
synapsedb:test> insert {"title":"JavaScript Tutorial","content":"Web development with JavaScript"}

# Enable search
synapsedb:test> index title content

# Test single field search
synapsedb:test> search title Java

# Test multi-field search
synapsedb:test> search-multi title,content programming

# Test phrase search
synapsedb:test> phrase content "from scratch"
```

---

## Tips for Effective Use

1. **Always enable indexing before searching**: Use `index` command first
2. **Use tab completion**: Press TAB to autocomplete commands
3. **Use arrow keys**: UP/DOWN to navigate command history
4. **Export regularly**: Data is in-memory only, export before exiting
5. **Check stats**: Use `stats` to see collection information
6. **Start simple**: Use `find` before complex searches
7. **Test queries**: Try searches with different terms

---

## Common Workflows

### Workflow 1: Load and Analyze Data
```
1. use mydata
2. load data.json
3. count
4. find
5. aggregate group by <field>
6. stats
```

### Workflow 2: Build Search Index
```
1. use products
2. insert <multiple documents>
3. index name description category
4. search description <query>
5. search-multi name,description <query>
```

### Workflow 3: Data Export/Backup
```
1. use collection
2. find (verify data)
3. export backup.json
4. (exit and restart CLI)
5. use collection
6. load backup.json
7. count (verify)
```

---

## Keyboard Shortcuts

- **TAB** - Autocomplete commands
- **UP/DOWN arrows** - Command history
- **Ctrl+C** - Cancel input (doesn't exit)
- **Ctrl+D** - Exit CLI
- Type `clear` - Clear screen

---

## Quick Reference Card

```
┌─────────────────────────────────────────────────────────┐
│ QUICK REFERENCE                                         │
├─────────────────────────────────────────────────────────┤
│ Start:     java -jar synapsedb-cli.jar                 │
│ Help:      help                                         │
│ Use:       use <collection>                             │
│ Load:      load <file.json>                             │
│ Index:     index <field1> <field2>                      │
│ Search:    search <field> <query>                       │
│ Find:      find <field>=<value>                         │
│ Insert:    insert {"field":"value"}                     │
│ Count:     count                                        │
│ Aggregate: aggregate group by <field>                   │
│ Export:    export <file.json>                           │
│ Stats:     stats                                        │
│ Exit:      exit                                         │
└─────────────────────────────────────────────────────────┘
```

---

## Next Steps

Now that you know how to use the CLI:

1. **Try the sample data**: Load and explore the books dataset
2. **Import your own data**: Create JSON files with your data
3. **Experiment with search**: Try different query combinations
4. **Build aggregations**: Analyze your data with grouping
5. **Export results**: Save your work before exiting

**Start exploring:**
```bash
cd /Users/amittiwari/Desktop/synapsedb
java -jar synapsedb-cli/target/synapsedb-cli-0.1.0-SNAPSHOT-jar-with-dependencies.jar
```

Then type `help` and follow the tutorial above!

Enjoy using SynapseDB CLI! 🚀

