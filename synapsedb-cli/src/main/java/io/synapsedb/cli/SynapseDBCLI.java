package io.synapsedb.cli;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import de.vandermeer.asciitable.AsciiTable;
import de.vandermeer.asciitable.CWC_LongestLine;
import io.synapsedb.SynapseDB;
import io.synapsedb.aggregation.AggregationPipeline;
import io.synapsedb.collection.Collection;
import io.synapsedb.document.Document;
import io.synapsedb.search.FullTextIndex;
import org.jline.reader.*;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

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
                "║              SynapseDB Interactive CLI                   ║\n" +
                "║                    Version 0.1.0                         ║\n" +
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
            case "search":
                searchDocuments(args);
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
            case "load":
                loadDataFromFile(args);
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
        System.out.println("  find [field=value]       - Find documents");
        System.out.println("  update <id> <json>       - Update a document");
        System.out.println("  delete <id>              - Delete a document");
        System.out.println("  count                    - Count documents in collection");

        System.out.println("\nSearch Operations:");
        System.out.println("  index <field1> <field2>  - Enable full-text search on fields");
        System.out.println("  search <field> <query>   - Full-text search");

        System.out.println("\nAggregation:");
        System.out.println("  aggregate <pipeline>     - Run aggregation pipeline");

        System.out.println("\nData Loading:");
        System.out.println("  load <file.json>         - Load documents from JSON file");

        System.out.println("\nUtility:");
        System.out.println("  clear                    - Clear screen");
        System.out.println("  help                     - Show this help message");
        System.out.println("  exit                     - Exit the CLI");

        System.out.println("\n=== Examples ===\n");
        System.out.println("  use products");
        System.out.println("  insert {\"name\":\"Laptop\",\"price\":1000,\"category\":\"Electronics\"}");
        System.out.println("  find category=Electronics");
        System.out.println("  index name description");
        System.out.println("  search name laptop");
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
        FullTextIndex.SearchResult result = collection.search(field, query, 20);

        System.out.println("\nFound " + result.getTotalMatches() + " results:\n");

        if (result.getResults().isEmpty()) {
            System.out.println("No documents found.");
        } else {
            for (FullTextIndex.ScoredDocument scored : result.getResults()) {
                Document doc = scored.getDocument();
                System.out.println(String.format("Score: %.4f | ID: %s", scored.getScore(), doc.getId()));
                System.out.println("  " + formatDocumentOneLine(doc));
                System.out.println();
            }
        }
    }

    private void aggregateDocuments(String pipelineSpec) {
        if (currentCollection == null) {
            System.out.println("Error: No collection selected. Use 'use <collection>' first.");
            return;
        }

        // For now, support simple aggregation: group by <field>
        if (pipelineSpec.startsWith("group by ")) {
            String field = pipelineSpec.substring(9).trim();

            AggregationPipeline pipeline = new AggregationPipeline()
                .addStage(AggregationPipeline.group(field).count("count"));

            Collection collection = database.collection(currentCollection);
            AggregationPipeline.AggregationResult result = collection.aggregate(pipeline);

            System.out.println("\nAggregation Results:\n");
            displayDocuments(result.getDocuments());
        } else {
            System.out.println("Usage: aggregate group by <field>");
            System.out.println("Example: aggregate group by category");
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

                for (var element : array) {
                    JsonObject jsonObject = element.getAsJsonObject();
                    Document doc = new Document();

                    for (String key : jsonObject.keySet()) {
                        doc.addField(key, extractValue(jsonObject.get(key)));
                    }

                    collection.insert(doc);
                    count++;
                }

                System.out.println("✓ Loaded " + count + " documents from " + filename);
            } else {
                // Single JSON object
                insertDocument(content);
            }
        } catch (IOException e) {
            System.out.println("Error: Could not read file - " + e.getMessage());
        } catch (Exception e) {
            System.out.println("Error: Invalid JSON in file - " + e.getMessage());
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
            "insert", "find", "search", "aggregate", "update", "delete",
            "count", "index", "load", "stats", "clear"
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

