package io.synapsedb.core.examples.operations;

import io.synapsedb.core.document.Document;
import io.synapsedb.core.document.FieldConfig;
import io.synapsedb.core.document.FieldType;
import io.synapsedb.core.index.Index;
import io.synapsedb.core.index.IndexManager;
import io.synapsedb.core.index.IndexSettings;
import io.synapsedb.core.index.operations.FlushOperations;
import io.synapsedb.core.index.operations.IndexOperations;
import io.synapsedb.core.index.operations.MergeOperations;
import io.synapsedb.core.index.operations.RefreshOperations;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TermQuery;

import java.util.ArrayList;
import java.util.List;

/**
 * Examples demonstrating the use of high-level operation classes.
 * Shows how to use IndexOperations, FlushOperations, MergeOperations, and RefreshOperations.
 *
 * @author Amit Tiwari
 */
public class OperationsExample {

    private static final String INDEX_NAME = "operations_demo";
    private static final String DATA_PATH = "/tmp/synapsedb-operations";

    public static void main(String[] args) {
        System.out.println("=".repeat(80));
        System.out.println("SynapseDB Operations API Demo");
        System.out.println("=".repeat(80));
        System.out.println();

        try {
            // Initialize
            IndexManager manager = IndexManager.getInstance(DATA_PATH);

            // Delete if exists
            try { manager.deleteIndex(INDEX_NAME); } catch (Exception e) { /* ignore */ }

            // Create index
            Index index = manager.createIndex(INDEX_NAME, IndexSettings.defaultSettings());
            System.out.println("✅ Index created: " + INDEX_NAME);
            System.out.println();

            // Demonstrate IndexOperations
            demonstrateIndexOperations(index);

            // Demonstrate RefreshOperations
            demonstrateRefreshOperations(index);

            // Demonstrate FlushOperations
            demonstrateFlushOperations(index);

            // Demonstrate MergeOperations
            demonstrateMergeOperations(index);

            // Cleanup
            index.close();
            System.out.println();
            System.out.println("=".repeat(80));
            System.out.println("🎉 Operations demo completed!");
            System.out.println("=".repeat(80));

        } catch (Exception e) {
            System.err.println("❌ Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void demonstrateIndexOperations(Index index) throws Exception {
        System.out.println("📦 IndexOperations Demo");
        System.out.println("─".repeat(80));

        IndexOperations ops = new IndexOperations(index);

        // 1. Bulk add documents
        System.out.println("\n1️⃣  Bulk Add Operation");
        List<Document> docs = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            docs.add(new Document("doc-" + i)
                .addField("title", "Document " + i, FieldConfig.builder().type(FieldType.TEXT).tokenized(true).build())
                .addField("content", "This is the content of document " + i, FieldConfig.builder().type(FieldType.TEXT).tokenized(true).build())
                .addField("number", (long) i, FieldConfig.builder().type(FieldType.LONG).tokenized(false).build())
            );
        }

        int added = ops.bulkAdd(docs);
        System.out.println("   ✓ Bulk added " + added + " documents");
        System.out.println("   ✓ Index has " + ops.getDocumentCount() + " documents");
        System.out.println("   ✓ Is empty? " + ops.isEmpty());

        // 2. Add and refresh
        System.out.println("\n2️⃣  Add and Refresh Operation");
        List<Document> moreDocs = new ArrayList<>();
        for (int i = 11; i <= 15; i++) {
            moreDocs.add(new Document("doc-" + i)
                .addField("title", "Document " + i, FieldConfig.builder().type(FieldType.TEXT).tokenized(true).build())
                .addField("content", "More content " + i, FieldConfig.builder().type(FieldType.TEXT).tokenized(true).build())
            );
        }

        int addedAndRefreshed = ops.addAndRefresh(moreDocs);
        System.out.println("   ✓ Added and refreshed " + addedAndRefreshed + " documents");
        System.out.println("   ✓ Documents are now immediately searchable");

        // 3. Search count
        System.out.println("\n3️⃣  Search Count Operation");
        long totalDocs = ops.searchCount(new MatchAllDocsQuery());
        long titleDocs = ops.searchCount(new TermQuery(new Term("title", "document")));
        System.out.println("   ✓ Total documents: " + totalDocs);
        System.out.println("   ✓ Documents with 'document' in title: " + titleDocs);

        // 4. Bulk update
        System.out.println("\n4️⃣  Bulk Update Operation");
        List<Document> updates = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            updates.add(new Document("doc-" + i)
                .addField("title", "Updated Document " + i, FieldConfig.builder().type(FieldType.TEXT).tokenized(true).build())
                .addField("content", "Updated content " + i, FieldConfig.builder().type(FieldType.TEXT).tokenized(true).build())
                .addField("updated", true, FieldConfig.builder().build())
            );
        }

        int updated = ops.bulkUpdate(updates);
        ops.commitAndRefresh();
        System.out.println("   ✓ Bulk updated " + updated + " documents");

        // 5. Bulk delete
        System.out.println("\n5️⃣  Bulk Delete Operation");
        List<String> idsToDelete = List.of("doc-11", "doc-12", "doc-13");
        int deleted = ops.bulkDelete(idsToDelete);
        ops.commitAndRefresh();
        System.out.println("   ✓ Bulk deleted " + deleted + " documents");
        System.out.println("   ✓ Remaining documents: " + ops.getDocumentCount());

        System.out.println();
    }

    private static void demonstrateRefreshOperations(Index index) throws Exception {
        System.out.println("🔄 RefreshOperations Demo");
        System.out.println("─".repeat(80));

        RefreshOperations refreshOps = new RefreshOperations(index);

        System.out.println("\n1️⃣  Refresh Statistics");
        System.out.println("   Auto-refresh enabled: " + refreshOps.isAutoRefreshEnabled());
        System.out.println("   Refresh interval: " + refreshOps.getRefreshIntervalMs() + "ms");
        System.out.println("   Total refreshes: " + refreshOps.getRefreshCount());
        System.out.println("   Average refresh time: " + String.format("%.2f", refreshOps.getAverageRefreshTimeMs()) + "ms");

        System.out.println("\n2️⃣  Manual Refresh");
        long refreshTime = refreshOps.refreshWithTiming();
        System.out.println("   ✓ Refresh completed in " + refreshTime + "ms");

        System.out.println("\n3️⃣  Refresh Status");
        long timeSinceRefresh = refreshOps.getTimeSinceLastRefreshMs();
        System.out.println("   Time since last refresh: " + timeSinceRefresh + "ms");
        System.out.println("   Is refresh needed? " + refreshOps.isRefreshNeeded());

        System.out.println("\n4️⃣  Complete Statistics");
        System.out.println("   " + refreshOps.getRefreshStatistics());

        System.out.println();
    }

    private static void demonstrateFlushOperations(Index index) throws Exception {
        System.out.println("💾 FlushOperations Demo");
        System.out.println("─".repeat(80));

        FlushOperations flushOps = new FlushOperations(index);

        System.out.println("\n1️⃣  Flush Statistics (Before)");
        System.out.println("   Total flushes: " + flushOps.getFlushCount());
        System.out.println("   Total flush time: " + flushOps.getTotalFlushTimeMs() + "ms");
        System.out.println("   Average flush time: " + String.format("%.2f", flushOps.getAverageFlushTimeMs()) + "ms");
        long timeSinceFlush = flushOps.getTimeSinceLastFlushMs();
        if (timeSinceFlush >= 0) {
            System.out.println("   Time since last flush: " + timeSinceFlush + "ms");
        } else {
            System.out.println("   Time since last flush: Never flushed");
        }

        System.out.println("\n2️⃣  Performing Flush");
        flushOps.flush();
        System.out.println("   ✓ Flush completed");

        System.out.println("\n3️⃣  Flush and Commit");
        flushOps.flushAndCommit();
        System.out.println("   ✓ Flushed and committed (durable)");

        System.out.println("\n4️⃣  Complete Cycle");
        flushOps.flushCommitAndRefresh();
        System.out.println("   ✓ Flushed, committed, and refreshed");

        System.out.println("\n5️⃣  Flush Statistics (After)");
        System.out.println("   Total flushes: " + flushOps.getFlushCount());
        System.out.println("   Average flush time: " + String.format("%.2f", flushOps.getAverageFlushTimeMs()) + "ms");

        System.out.println();
    }

    private static void demonstrateMergeOperations(Index index) throws Exception {
        System.out.println("🔀 MergeOperations Demo");
        System.out.println("─".repeat(80));

        MergeOperations mergeOps = new MergeOperations(index);

        System.out.println("\n1️⃣  Merge Statistics (Before)");
        System.out.println("   Total merges: " + mergeOps.getMergeCount());
        System.out.println("   Total merge time: " + mergeOps.getTotalMergeTimeMs() + "ms");
        System.out.println("   Average merge time: " + String.format("%.2f", mergeOps.getAverageMergeTimeMs()) + "ms");
        System.out.println("   Total bytes merged: " + formatBytes(mergeOps.getTotalMergeBytes()));
        System.out.println("   Merges in progress: " + mergeOps.getCurrentMergeCount());

        System.out.println("\n2️⃣  Force Merge to 5 Segments");
        mergeOps.forceMerge(5);
        System.out.println("   ✓ Merged to 5 segments");

        System.out.println("\n3️⃣  Optimal Merge");
        mergeOps.mergeToOptimalSegments();
        System.out.println("   ✓ Merged to optimal number of segments");

        System.out.println("\n4️⃣  Full Optimization (Single Segment)");
        System.out.println("   ⚠️  This can be expensive for large indices!");
        mergeOps.optimize();
        System.out.println("   ✓ Optimized to single segment");

        System.out.println("\n5️⃣  Optimize and Commit");
        mergeOps.optimizeAndCommit();
        System.out.println("   ✓ Optimized and committed");

        System.out.println("\n6️⃣  Merge Statistics (After)");
        System.out.println("   Total merges: " + mergeOps.getMergeCount());
        System.out.println("   Average merge time: " + String.format("%.2f", mergeOps.getAverageMergeTimeMs()) + "ms");
        System.out.println("   Total bytes merged: " + formatBytes(mergeOps.getTotalMergeBytes()));
        System.out.println("   Currently merging: " + mergeOps.isMergeInProgress());

        System.out.println();
    }

    private static String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(1024));
        char pre = "KMGTPE".charAt(exp - 1);
        return String.format("%.2f %sB", bytes / Math.pow(1024, exp), pre);
    }
}

