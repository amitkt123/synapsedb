package io.synapsedb.core.cli;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.synapsedb.core.SynapseDB;
import io.synapsedb.core.aggregation.AggregationPipeline;
import io.synapsedb.core.analysis.Analyzer;
import io.synapsedb.core.analysis.analyser.LemmatizationAnalyzer;
import io.synapsedb.core.analysis.analyser.StandardAnalyzer;
import io.synapsedb.core.analysis.analyser.StemmingAnalyzer;
import io.synapsedb.core.collection.Collection;
import io.synapsedb.core.document.Document;
import io.synapsedb.core.search.FullTextIndex;
import org.jline.reader.*;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * Interactive CLI for SynapseDB
 *
 * @author Amit Tiwari
 */
@Command(name = "synapsedb", mixinStandardHelpOptions = true, version = "SynapseDB CLI 0.1.0",
        description = "Interactive command-line interface for SynapseDB")
public class SynapseDBCLI implements Runnable {

    private SynapseDB database;
    private String currentCollection;
    private final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    private Terminal terminal;
    private LineReader lineReader;
    private boolean running = true;

    @Option(names = {"-d", "--database"}, description = "Database name", defaultValue = "mydb")
    private String databaseName;

    public static void main(String[] args) {
        int exitCode = new CommandLine(new SynapseDBCLI()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public void run() {
        try {
            initializeTerminal();
            initializeDatabase();
            printWelcomeBanner();
            startInteractiveMode();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            cleanup();
        }
    }

    private void initializeTerminal() throws IOException {
        terminal = TerminalBuilder.builder()
                .system(true)
                .build();

        lineReader = LineReaderBuilder.builder()
                .terminal(terminal)
                .completer(new CommandCompleter())
                .build();
    }

    private void initializeDatabase() {
        database = new SynapseDB(databaseName);
        System.out.println("✓ Connected to database: " + databaseName);
    }

    private void printWelcomeBanner() {
        System.out.println("\n" +
                "╔═══════════════════════════════════════════════════════════╗\n" +
                "║              SynapseDB Interactive CLI                    ║\n" +
                "║                    Version 0.1.0                          ║\n" +
                "╚═══════════════════════════════════════════════════════════╝\n");
        System.out.println("Type 'help' for available commands or 'exit' to quit.\n");
    }

    private void startInteractiveMode() {
        while (running) {
            try {
                String prompt = currentCollection != null
                    ? String.format("synapsedb:%s> ", currentCollection)
                    : "synapsedb> ";

                String line = lineReader.readLine(prompt);

                if (line == null || line.trim().isEmpty()) {
                    continue;
                }

                processCommand(line.trim());

            } catch (UserInterruptException e) {
                // Ctrl+C pressed
                System.out.println("\nUse 'exit' to quit");
            } catch (EndOfFileException e) {
                // Ctrl+D pressed
                break;
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
            }
        }
    }

    private void processCommand(String input) {
        String[] parts = input.split("\\s+", 2);
        String command = parts[0].toLowerCase();
        String args = parts.length > 1 ? parts[1] : "";

        switch (command) {
            case "help":
                showHelp();
                break;
            case "exit":
            case "quit":
                running = false;
                System.out.println("Goodbye!");
                break;
            case "use":
                useCollection(args);
                break;
            case "show":
                showCollections();
                break;
            case "create":
                createCollection(args);
                break;
            case "drop":
                dropCollection(args);
                break;
            case "insert":
                insertDocument(args);
                break;
            case "find":
                findDocuments(args);
                break;
            case "findbyid":
                findById(args);
                break;
            case "search":
                searchDocuments(args);
                break;
            case "search-multi":
                searchMultiField(args);
                break;
            case "phrase":
                phraseSearch(args);
                break;
            case "aggregate":
                aggregateDocuments(args);
                break;
            case "update":
                updateDocument(args);
                break;
            case "delete":
                deleteDocument(args);
                break;
            case "count":
                countDocuments();
                break;
            case "index":
                enableFullTextSearch(args);
                break;
            case "analyzer":
                setAnalyzer(args);
                break;
            case "load":
                loadDataFromFile(args);
                break;
            case "export":
                exportToFile(args);
                break;
            case "stats":
                showStats();
                break;
            case "clear":
                clearScreen();
                break;
            default:
                System.out.println("Unknown command: " + command);
                System.out.println("Type 'help' for available commands.");
        }
    }

    private void showHelp() {
        System.out.println("\n=== Available Commands ===\n");

        System.out.println("Database Operations:");
        System.out.println("  show                     - List all collections");
        System.out.println("  use <collection>         - Switch to a collection");
        System.out.println("  create <collection>      - Create a new collection");
        System.out.println("  drop <collection>        - Drop a collection");
        System.out.println("  stats                    - Show database statistics");

        System.out.println("\nDocument Operations:");
        System.out.println("  insert <json>            - Insert a document");
        System.out.println("  find [field=value]       - Find documents (or all if no query)");
        System.out.println("  findById <id>            - Find document by ID");
        System.out.println("  update <id> <json>       - Update a document");
        System.out.println("  delete <id>              - Delete a document");
        System.out.println("  count                    - Count documents in collection");

        System.out.println("\nSearch Operations:");
        System.out.println("  index <field1> <field2>  - Enable full-text search on fields");
        System.out.println("  analyzer <type>          - Set text analyzer: standard, stemming, lemmatization");
        System.out.println("  search <field> <query>   - Full-text search in single field");
        System.out.println("  search-multi <fields> <query> - Search in multiple fields (comma-separated)");
        System.out.println("  phrase <field> <phrase>  - Exact phrase search");

        System.out.println("\nAggregation:");
        System.out.println("  aggregate group by <field>           - Group by field and count");
        System.out.println("  aggregate avg <field> by <groupField> - Average aggregation");
        System.out.println("  aggregate sum <field> by <groupField> - Sum aggregation");

        System.out.println("\nData Import/Export:");
        System.out.println("  load <file.json>         - Load documents from JSON file");
        System.out.println("  export <file.json>       - Export all documents to JSON file");

        System.out.println("\nUtility:");
        System.out.println("  clear                    - Clear screen");
        System.out.println("  help                     - Show this help message");
        System.out.println("  exit / quit              - Exit the CLI");

        System.out.println("\n=== Examples ===\n");
        System.out.println("  use books");
        System.out.println("  load sample-data/books.json");
        System.out.println("  analyzer stemming");
        System.out.println("  index title author description");
        System.out.println("  search description \"machine learning\"");
        System.out.println("  search-multi title,description python");
        System.out.println("  phrase description \"comprehensive guide\"");
        System.out.println("  aggregate group by category");
        System.out.println("  aggregate avg price by category");
        System.out.println("  find category=Programming");
        System.out.println("  export output/my-books.json");
        System.out.println();
    }

    private void useCollection(String name) {
        if (name.isEmpty()) {
            System.out.println("Usage: use <collection_name>");
            return;
        }
        currentCollection = name;
        database.collection(name); // Create if doesn't exist
        System.out.println("✓ Switched to collection: " + name);
    }

    private void showCollections() {
        Set<String> collections = database.listCollections();
        if (collections.isEmpty()) {
            System.out.println("No collections found.");
        } else {
            System.out.println("\nCollections:");
            for (String col : collections) {
                Collection c = database.getCollection(col);
                long count = c != null ? c.count() : 0;
                String current = col.equals(currentCollection) ? " *" : "";
                System.out.println("  - " + col + " (" + count + " documents)" + current);
            }
            System.out.println();
        }
    }

    private void createCollection(String name) {
        if (name.isEmpty()) {
            System.out.println("Usage: create <collection_name>");
            return;
        }
        database.collection(name);
        System.out.println("✓ Collection created: " + name);
    }

    private void dropCollection(String name) {
        if (name.isEmpty()) {
            System.out.println("Usage: drop <collection_name>");
            return;
        }
        if (database.dropCollection(name)) {
            System.out.println("✓ Collection dropped: " + name);
            if (name.equals(currentCollection)) {
                currentCollection = null;
            }
        } else {
            System.out.println("Collection not found: " + name);
        }
    }

    private void insertDocument(String json) {
        if (currentCollection == null) {
            System.out.println("Error: No collection selected. Use 'use <collection>' first.");
            return;
        }

        try {
            JsonObject jsonObject = JsonParser.parseString(json).getAsJsonObject();
            Document doc = new Document();

            for (String key : jsonObject.keySet()) {
                Object value = extractValue(jsonObject.get(key));
                doc.addField(key, value);
            }

            Collection collection = database.collection(currentCollection);
            collection.insert(doc);

            System.out.println("✓ Document inserted with ID: " + doc.getId());
        } catch (Exception e) {
            System.out.println("Error: Invalid JSON - " + e.getMessage());
        }
    }

    private void findDocuments(String query) {
        if (currentCollection == null) {
            System.out.println("Error: No collection selected. Use 'use <collection>' first.");
            return;
        }

        Collection collection = database.collection(currentCollection);
        List<Document> results;

        if (query.isEmpty()) {
            // Find all
            results = collection.findAll();
        } else if (query.contains("=")) {
            // Simple field=value query
            String[] parts = query.split("=", 2);
            results = collection.find(parts[0].trim(), parts[1].trim());
        } else {
            System.out.println("Error: Invalid query format. Use: field=value");
            return;
        }

        displayDocuments(results);
    }

    private void findById(String id) {
        if (currentCollection == null) {
            System.out.println("Error: No collection selected. Use 'use <collection>' first.");
            return;
        }

        if (id.isEmpty()) {
            System.out.println("Usage: findById <id>");
            return;
        }

        Collection collection = database.collection(currentCollection);
        Document doc = collection.findById(id);

        if (doc == null) {
            System.out.println("Document not found with ID: " + id);
        } else {
            displayDocuments(Arrays.asList(doc));
        }
    }

    private void searchDocuments(String args) {
        if (currentCollection == null) {
            System.out.println("Error: No collection selected. Use 'use <collection>' first.");
            return;
        }

        String[] parts = args.split("\\s+", 2);
        if (parts.length < 2) {
            System.out.println("Usage: search <field> <query>");
            return;
        }

        String field = parts[0];
        String query = parts[1];

        Collection collection = database.collection(currentCollection);
        long startTime = System.currentTimeMillis();
        FullTextIndex.SearchResult result = collection.search(field, query, 20);
        long duration = System.currentTimeMillis() - startTime;

        System.out.println("\nFound " + result.getTotalMatches() + " results in " + duration + "ms:\n");

        if (result.getResults().isEmpty()) {
            System.out.println("No documents found.");
        } else {
            displaySearchResults(result);
        }
    }

    private void searchMultiField(String args) {
        if (currentCollection == null) {
            System.out.println("Error: No collection selected. Use 'use <collection>' first.");
            return;
        }

        String[] parts = args.split("\\s+", 2);
        if (parts.length < 2) {
            System.out.println("Usage: search-multi <field1,field2,...> <query>");
            return;
        }

        String[] fields = parts[0].split(",");
        String query = parts[1];

        Collection collection = database.collection(currentCollection);
        long startTime = System.currentTimeMillis();
        FullTextIndex.SearchResult result = collection.searchMultiple(Arrays.asList(fields), query, 20);
        long duration = System.currentTimeMillis() - startTime;

        System.out.println("\nSearching in fields: " + String.join(", ", fields));
        System.out.println("Found " + result.getTotalMatches() + " results in " + duration + "ms:\n");

        if (result.getResults().isEmpty()) {
            System.out.println("No documents found.");
        } else {
            displaySearchResults(result);
        }
    }

    private void phraseSearch(String args) {
        if (currentCollection == null) {
            System.out.println("Error: No collection selected. Use 'use <collection>' first.");
            return;
        }

        String[] parts = args.split("\\s+", 2);
        if (parts.length < 2) {
            System.out.println("Usage: phrase <field> <\"exact phrase\">");
            return;
        }

        String field = parts[0];
        String phrase = parts[1].replaceAll("^\"|\"$", ""); // Remove quotes if present

        Collection collection = database.collection(currentCollection);
        long startTime = System.currentTimeMillis();
        FullTextIndex.SearchResult result = collection.phraseSearch(field, phrase, 20);
        long duration = System.currentTimeMillis() - startTime;

        System.out.println("\nPhrase search for: \"" + phrase + "\" in field: " + field);
        System.out.println("Found " + result.getTotalMatches() + " results in " + duration + "ms:\n");

        if (result.getResults().isEmpty()) {
            System.out.println("No documents found.");
        } else {
            displaySearchResults(result);
        }
    }

    private void displaySearchResults(FullTextIndex.SearchResult result) {
        for (FullTextIndex.ScoredDocument scored : result.getResults()) {
            Document doc = scored.getDocument();
            System.out.println(String.format("Score: %.4f | ID: %s", scored.getScore(), doc.getId()));

            // Display key fields
            for (String fieldName : doc.getFieldNames()) {
                Object value = doc.getField(fieldName);
                if (value != null) {
                    String valueStr = value.toString();
                    if (valueStr.length() > 100) {
                        valueStr = valueStr.substring(0, 97) + "...";
                    }
                    System.out.println("  " + fieldName + ": " + valueStr);
                }
            }
            System.out.println();
        }
    }

    private void aggregateDocuments(String pipelineSpec) {
        if (currentCollection == null) {
            System.out.println("Error: No collection selected. Use 'use <collection>' first.");
            return;
        }

        Collection collection = database.collection(currentCollection);
        AggregationPipeline pipeline = new AggregationPipeline();

        try {
            // Parse aggregation commands
            if (pipelineSpec.startsWith("group by ")) {
                // Simple group by: aggregate group by category
                String field = pipelineSpec.substring(9).trim();
                pipeline.addStage(AggregationPipeline.group(field).count("count"));

            } else if (pipelineSpec.matches("avg .+ by .+")) {
                // Average: aggregate avg price by category
                String[] parts = pipelineSpec.split(" by ");
                String avgField = parts[0].substring(4).trim();
                String groupField = parts[1].trim();
                pipeline.addStage(AggregationPipeline.group(groupField)
                    .avg("average_" + avgField, avgField)
                    .count("count"));

            } else if (pipelineSpec.matches("sum .+ by .+")) {
                // Sum: aggregate sum price by category
                String[] parts = pipelineSpec.split(" by ");
                String sumField = parts[0].substring(4).trim();
                String groupField = parts[1].trim();
                pipeline.addStage(AggregationPipeline.group(groupField)
                    .sum("total_" + sumField, sumField)
                    .count("count"));

            } else if (pipelineSpec.matches("min .+ by .+")) {
                // Min: aggregate min price by category
                String[] parts = pipelineSpec.split(" by ");
                String minField = parts[0].substring(4).trim();
                String groupField = parts[1].trim();
                pipeline.addStage(AggregationPipeline.group(groupField)
                    .min("min_" + minField, minField)
                    .count("count"));

            } else if (pipelineSpec.matches("max .+ by .+")) {
                // Max: aggregate max price by category
                String[] parts = pipelineSpec.split(" by ");
                String maxField = parts[0].substring(4).trim();
                String groupField = parts[1].trim();
                pipeline.addStage(AggregationPipeline.group(groupField)
                    .max("max_" + maxField, maxField)
                    .count("count"));

            } else {
                System.out.println("Usage:");
                System.out.println("  aggregate group by <field>");
                System.out.println("  aggregate avg <field> by <groupField>");
                System.out.println("  aggregate sum <field> by <groupField>");
                System.out.println("  aggregate min <field> by <groupField>");
                System.out.println("  aggregate max <field> by <groupField>");
                return;
            }

            long startTime = System.currentTimeMillis();
            AggregationPipeline.AggregationResult result = collection.aggregate(pipeline);
            long duration = System.currentTimeMillis() - startTime;

            System.out.println("\nAggregation completed in " + duration + "ms\n");
            displayDocuments(result.getDocuments());

        } catch (Exception e) {
            System.out.println("Error executing aggregation: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void updateDocument(String args) {
        if (currentCollection == null) {
            System.out.println("Error: No collection selected. Use 'use <collection>' first.");
            return;
        }

        String[] parts = args.split("\\s+", 2);
        if (parts.length < 2) {
            System.out.println("Usage: update <id> <json>");
            return;
        }

        String id = parts[0];
        String json = parts[1];

        try {
            JsonObject jsonObject = JsonParser.parseString(json).getAsJsonObject();
            Map<String, Object> updates = new HashMap<>();

            for (String key : jsonObject.keySet()) {
                updates.put(key, extractValue(jsonObject.get(key)));
            }

            Collection collection = database.collection(currentCollection);
            if (collection.update(id, updates)) {
                System.out.println("✓ Document updated: " + id);
            } else {
                System.out.println("Document not found: " + id);
            }
        } catch (Exception e) {
            System.out.println("Error: Invalid JSON - " + e.getMessage());
        }
    }

    private void deleteDocument(String id) {
        if (currentCollection == null) {
            System.out.println("Error: No collection selected. Use 'use <collection>' first.");
            return;
        }

        if (id.isEmpty()) {
            System.out.println("Usage: delete <id>");
            return;
        }

        Collection collection = database.collection(currentCollection);
        if (collection.delete(id)) {
            System.out.println("✓ Document deleted: " + id);
        } else {
            System.out.println("Document not found: " + id);
        }
    }

    private void countDocuments() {
        if (currentCollection == null) {
            System.out.println("Error: No collection selected. Use 'use <collection>' first.");
            return;
        }

        Collection collection = database.collection(currentCollection);
        System.out.println("Document count: " + collection.count());
    }

    private void enableFullTextSearch(String fields) {
        if (currentCollection == null) {
            System.out.println("Error: No collection selected. Use 'use <collection>' first.");
            return;
        }

        if (fields.isEmpty()) {
            System.out.println("Usage: index <field1> <field2> ...");
            return;
        }

        String[] fieldArray = fields.split("\\s+");
        Collection collection = database.collection(currentCollection);
        collection.enableFullTextSearch(fieldArray);

        System.out.println("✓ Full-text search enabled on fields: " + String.join(", ", fieldArray));
    }

    private void setAnalyzer(String analyzerType) {
        if (currentCollection == null) {
            System.out.println("Error: No collection selected. Use 'use <collection>' first.");
            return;
        }

        if (analyzerType.isEmpty()) {
            // Show current analyzer
            Collection collection = database.collection(currentCollection);
            Analyzer current = collection.getAnalyzer();
            System.out.println("Current analyzer: " + current.getName());
            System.out.println("\nAvailable analyzers:");
            System.out.println("  standard       - Basic tokenization + lowercase");
            System.out.println("  stemming       - Porter stemming (removes suffixes)");
            System.out.println("  lemmatization  - Dictionary-based word reduction");
            System.out.println("\nUsage: analyzer <type>");
            return;
        }

        Collection collection = database.collection(currentCollection);
        Analyzer analyzer;

        switch (analyzerType.toLowerCase()) {
            case "standard":
                analyzer = new StandardAnalyzer();
                break;
            case "stemming":
            case "stem":
                analyzer = new StemmingAnalyzer();
                break;
            case "lemmatization":
            case "lemma":
                analyzer = new LemmatizationAnalyzer();
                break;
            default:
                System.out.println("Unknown analyzer: " + analyzerType);
                System.out.println("Available: standard, stemming, lemmatization");
                return;
        }

        collection.setAnalyzer(analyzer);
        System.out.println("✓ Analyzer set to: " + analyzer.getName());
        System.out.println("  (Documents will be re-indexed automatically)");
    }

    private void loadDataFromFile(String filename) {
        if (currentCollection == null) {
            System.out.println("Error: No collection selected. Use 'use <collection>' first.");
            return;
        }

        try {
            String content = Files.readString(Paths.get(filename));

            // Try to parse as JSON array
            if (content.trim().startsWith("[")) {
                com.google.gson.JsonArray array = JsonParser.parseString(content).getAsJsonArray();
                Collection collection = database.collection(currentCollection);
                int count = 0;

                System.out.print("Loading documents");
                for (var element : array) {
                    JsonObject jsonObject = element.getAsJsonObject();
                    Document doc = new Document();

                    for (String key : jsonObject.keySet()) {
                        doc.addField(key, extractValue(jsonObject.get(key)));
                    }

                    collection.insert(doc);
                    count++;
                    if (count % 10 == 0) {
                        System.out.print(".");
                    }
                }

                System.out.println("\n✓ Loaded " + count + " documents from " + filename);
            } else {
                // Single JSON object
                insertDocument(content);
            }
        } catch (IOException e) {
            System.out.println("Error: Could not read file - " + e.getMessage());
        } catch (Exception e) {
            System.out.println("Error: Invalid JSON in file - " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void exportToFile(String filename) {
        if (currentCollection == null) {
            System.out.println("Error: No collection selected. Use 'use <collection>' first.");
            return;
        }

        if (filename.isEmpty()) {
            System.out.println("Usage: export <filename.json>");
            return;
        }

        try {
            Collection collection = database.collection(currentCollection);
            List<Document> documents = collection.findAll();

            if (documents.isEmpty()) {
                System.out.println("No documents to export.");
                return;
            }

            // Convert documents to JSON
            com.google.gson.JsonArray jsonArray = new com.google.gson.JsonArray();
            for (Document doc : documents) {
                JsonObject jsonObject = new JsonObject();
                for (String fieldName : doc.getFieldNames()) {
                    Object value = doc.getField(fieldName);
                    if (value instanceof Number) {
                        jsonObject.addProperty(fieldName, (Number) value);
                    } else if (value instanceof Boolean) {
                        jsonObject.addProperty(fieldName, (Boolean) value);
                    } else {
                        jsonObject.addProperty(fieldName, value.toString());
                    }
                }
                jsonArray.add(jsonObject);
            }

            // Write to file
            String json = gson.toJson(jsonArray);
            Files.writeString(Paths.get(filename), json);

            System.out.println("✓ Exported " + documents.size() + " documents to " + filename);

        } catch (IOException e) {
            System.out.println("Error: Could not write to file - " + e.getMessage());
        } catch (Exception e) {
            System.out.println("Error: Export failed - " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void showStats() {
        SynapseDB.DatabaseStats stats = database.getStats();
        System.out.println("\n=== Database Statistics ===");
        System.out.println("Database: " + stats.getName());
        System.out.println("Collections: " + stats.getCollectionCount());
        System.out.println("Total Documents: " + stats.getTotalDocuments());

        if (currentCollection != null) {
            Collection collection = database.getCollection(currentCollection);
            if (collection != null) {
                Collection.CollectionStats colStats = collection.getStats();
                System.out.println("\nCurrent Collection: " + currentCollection);
                System.out.println("  Documents: " + colStats.getDocumentCount());
                System.out.println("  Searchable Fields: " + colStats.getSearchableFieldsCount());
            }
        }
        System.out.println();
    }

    private void clearScreen() {
        try {
            terminal.puts(org.jline.utils.InfoCmp.Capability.clear_screen);
            terminal.flush();
        } catch (Exception e) {
            // Fallback: print newlines
            for (int i = 0; i < 50; i++) {
                System.out.println();
            }
        }
    }

    private void displayDocuments(List<Document> documents) {
        if (documents.isEmpty()) {
            System.out.println("No documents found.");
            return;
        }

        System.out.println("\nFound " + documents.size() + " document(s):\n");

        // Use simple formatting
        for (Document doc : documents) {
            System.out.println("ID: " + doc.getId());
            for (String fieldName : doc.getFieldNames()) {
                Object value = doc.getField(fieldName);
                System.out.println("  " + fieldName + ": " + value);
            }
            System.out.println("---");
        }
        System.out.println();
    }

    private String formatDocumentOneLine(Document doc) {
        StringBuilder sb = new StringBuilder();
        int count = 0;
        for (String fieldName : doc.getFieldNames()) {
            if (count > 0) sb.append(", ");
            if (count >= 3) {
                sb.append("...");
                break;
            }
            sb.append(fieldName).append("=").append(doc.getField(fieldName));
            count++;
        }
        return sb.toString();
    }

    private Object extractValue(com.google.gson.JsonElement element) {
        if (element.isJsonPrimitive()) {
            com.google.gson.JsonPrimitive primitive = element.getAsJsonPrimitive();
            if (primitive.isNumber()) {
                return primitive.getAsNumber();
            } else if (primitive.isBoolean()) {
                return primitive.getAsBoolean();
            } else {
                return primitive.getAsString();
            }
        }
        return element.toString();
    }

    private void cleanup() {
        if (database != null) {
            database.close();
        }
        if (terminal != null) {
            try {
                terminal.close();
            } catch (IOException e) {
                // Ignore
            }
        }
    }

    /**
     * Command completer for auto-completion
     */
    private static class CommandCompleter implements Completer {
        private static final List<String> COMMANDS = Arrays.asList(
            "help", "exit", "quit", "use", "show", "create", "drop",
            "insert", "find", "findById", "search", "search-multi", "phrase",
            "aggregate", "update", "delete", "count", "index", "analyzer", "load", "export",
            "stats", "clear"
        );

        @Override
        public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates) {
            String word = line.word();
            COMMANDS.stream()
                .filter(cmd -> cmd.startsWith(word))
                .forEach(cmd -> candidates.add(new Candidate(cmd)));
        }
    }
}

