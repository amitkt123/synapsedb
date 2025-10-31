package io.synapsedb.index;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Main entry point for index management in SynapseDB.
 * Provides high-level API for creating, managing, and operating on indices.
 * Manages lifecycle of indices, creation, deletion and retrieval
 * @author Amit Tiwari
 */
public class IndexManager implements Closeable {

    private static IndexManager instance;

    private final Path basePath;
    private final IndexRegistry registry;
    private final ReadWriteLock globalLock;
    private volatile boolean closed = false;

    /**
     * Private constructor for singleton
     */
    private IndexManager(Path basePath) throws IOException {
        this.basePath = basePath;
        this.registry = new IndexRegistry(basePath);
        this.globalLock = new ReentrantReadWriteLock();

        // Discover existing indices
        registry.discoverIndices();
    }

    /**
     * Get or create the IndexManager instance
     */
    public static synchronized IndexManager getInstance(String basePath) throws IOException {
        if (instance == null) {
            instance = new IndexManager(Paths.get(basePath));
        }
        return instance;
    }

    /**
     * Get the current instance (must be initialized first)
     */
    public static IndexManager getInstance() {
        if (instance == null) {
            throw new IllegalStateException("IndexManager not initialized. Call getInstance(basePath) first.");
        }
        return instance;
    }

    // ==================== Index Lifecycle ====================

    /**
     * Create a new index with default settings
     */
    public Index createIndex(String indexName) throws IndexException {
        return createIndex(indexName, IndexSettings.defaultSettings());
    }

    /**
     * Create a new index with custom settings
     */
    public Index createIndex(String indexName, IndexSettings settings) throws IndexException {
        validateIndexName(indexName);
        checkNotClosed();

        globalLock.writeLock().lock();
        try {
            if (registry.indexExists(indexName)) {
                throw new IndexAlreadyExistsException(indexName);
            }

            // Create the index
            Index index = new Index(indexName, basePath, settings);

            // Register it
            registry.registerIndex(index);

            return index;

        } catch (IOException e) {
            throw new IndexCreationException(indexName, e);
        } finally {
            globalLock.writeLock().unlock();
        }
    }

    /**
     * Delete an index
     */
    public void deleteIndex(String indexName) throws IndexException {
        checkNotClosed();

        globalLock.writeLock().lock();
        try {
            Index index = registry.getIndex(indexName);

            // Check if index can be deleted
            if (!index.getState().isDeletable()) {
                // Try to close it first
                if (index.getState() == IndexState.OPEN) {
                    index.close();
                } else {
                    throw new InvalidIndexStateException(indexName, index.getState(), "delete");
                }
            }

            // Update state
            index.getMetadata().setState(IndexState.DELETING);

            // Close the index if not already closed
            if (!index.isClosed()) {
                index.close();
            }

            // Unregister from registry
            registry.unregisterIndex(indexName);

            // Delete the physical directory
            Path indexPath = index.getIndexPath();
            deleteDirectory(indexPath);

        } catch (IOException e) {
            throw new IndexDeletionException(indexName, e);
        } finally {
            globalLock.writeLock().unlock();
        }
    }

    /**
     * Open a closed index
     */
    public void openIndex(String indexName) throws IndexException {
        checkNotClosed();

        globalLock.readLock().lock();
        try {
            Index index = registry.getIndex(indexName);

            if (!index.getState().canOpen()) {
                throw new InvalidIndexStateException(indexName, index.getState(), "open");
            }

            index.open();

        } catch (IOException e) {
            throw new IndexException("Failed to open index: " + indexName, e);
        } finally {
            globalLock.readLock().unlock();
        }
    }

    /**
     * Close an open index
     */
    public void closeIndex(String indexName) throws IndexException {
        checkNotClosed();

        globalLock.readLock().lock();
        try {
            Index index = registry.getIndex(indexName);

            if (!index.getState().canClose()) {
                throw new InvalidIndexStateException(indexName, index.getState(), "close");
            }

            index.close();

        } catch (IOException e) {
            throw new IndexException("Failed to close index: " + indexName, e);
        } finally {
            globalLock.readLock().unlock();
        }
    }

    /**
     * Check if an index exists
     */
    public boolean indexExists(String indexName) {
        checkNotClosed();
        return registry.indexExists(indexName);
    }

    /**
     * Get an index
     */
    public Index getIndex(String indexName) throws IndexNotFoundException {
        checkNotClosed();
        return registry.getIndex(indexName);
    }

    /**
     * Get all index names
     */
    public Set<String> getAllIndexNames() {
        checkNotClosed();
        return registry.getIndexNames();
    }

    /**
     * Get all indices
     */
    public Collection<Index> getAllIndices() {
        checkNotClosed();
        return registry.getAllIndices();
    }

    /**
     * Get indices matching a pattern (e.g., "logs-*")
     */
    public List<Index> getIndicesByPattern(String pattern) {
        checkNotClosed();
        return registry.getIndicesByPattern(pattern);
    }

    // ==================== Index Operations ====================

    /**
     * Refresh an index
     */
    public void refreshIndex(String indexName) throws IndexException {
        checkNotClosed();

        try {
            Index index = registry.getIndex(indexName);
            index.refresh();
        } catch (IOException e) {
            throw new IndexException("Failed to refresh index: " + indexName, e);
        }
    }

    /**
     * Refresh multiple indices
     */
    public void refreshIndices(String... indexNames) throws IndexException {
        checkNotClosed();

        List<IndexException> exceptions = new ArrayList<>();

        for (String indexName : indexNames) {
            try {
                refreshIndex(indexName);
            } catch (IndexException e) {
                exceptions.add(e);
            }
        }

        if (!exceptions.isEmpty()) {
            IndexException combined = new IndexException("Failed to refresh some indices");
            exceptions.forEach(combined::addSuppressed);
            throw combined;
        }
    }

    /**
     * Refresh all indices
     */
    public void refreshAll() throws IndexException {
        checkNotClosed();

        String[] allIndices = getAllIndexNames().toArray(new String[0]);
        refreshIndices(allIndices);
    }

    /**
     * Flush an index
     */
    public void flushIndex(String indexName) throws IndexException {
        checkNotClosed();

        try {
            Index index = registry.getIndex(indexName);
            index.flush();
        } catch (IOException e) {
            throw new IndexException("Failed to flush index: " + indexName, e);
        }
    }

    /**
     * Force merge an index
     */
    public void forceMergeIndex(String indexName, int maxNumSegments) throws IndexException {
        checkNotClosed();

        try {
            Index index = registry.getIndex(indexName);
            index.forceMerge(maxNumSegments);
        } catch (IOException e) {
            throw new IndexException("Failed to force merge index: " + indexName, e);
        }
    }

    // ==================== Alias Management ====================

    /**
     * Add an alias to an index
     */
    public void addAlias(String indexName, String alias) throws IndexException {
        checkNotClosed();

        globalLock.writeLock().lock();
        try {
            registry.addAlias(alias, indexName);
        } finally {
            globalLock.writeLock().unlock();
        }
    }

    /**
     * Remove an alias from an index
     */
    public void removeAlias(String indexName, String alias) throws IndexException {
        checkNotClosed();

        globalLock.writeLock().lock();
        try {
            registry.removeAlias(alias, indexName);
        } finally {
            globalLock.writeLock().unlock();
        }
    }

    /**
     * Swap an alias from one index to another
     */
    public void swapAlias(String alias, String fromIndex, String toIndex) throws IndexException {
        checkNotClosed();

        globalLock.writeLock().lock();
        try {
            registry.swapAlias(alias, fromIndex, toIndex);
        } finally {
            globalLock.writeLock().unlock();
        }
    }

    /**
     * Get all aliases
     */
    public Map<String, Set<String>> getAllAliases() {
        checkNotClosed();
        return registry.getAllAliases();
    }

    // ==================== Statistics ====================

    /**
     * Get index statistics
     */
    public IndexStats getIndexStats(String indexName) throws IndexNotFoundException {
        checkNotClosed();
        Index index = registry.getIndex(indexName);
        return index.getStats();
    }

    /**
     * Get index metadata
     */
    public IndexMetadata getIndexMetadata(String indexName) throws IndexNotFoundException {
        checkNotClosed();
        return registry.getIndexMetadata(indexName);
    }

    /**
     * Get cluster-wide statistics
     */
    public Map<String, Object> getClusterStats() {
        checkNotClosed();

        Map<String, Object> stats = new HashMap<>();
        stats.put("total_indices", getAllIndexNames().size());
        stats.put("base_path", basePath.toString());

        // Aggregate stats
        long totalDocs = 0;
        long totalSize = 0;
        int openIndices = 0;
        int closedIndices = 0;

        for (Index index : getAllIndices()) {
            IndexStats indexStats = index.getStats();
            totalDocs += indexStats.getTotalDocs();
            totalSize += indexStats.getStoreSizeInBytes();

            if (index.getState() == IndexState.OPEN) {
                openIndices++;
            } else if (index.getState() == IndexState.CLOSED) {
                closedIndices++;
            }
        }

        stats.put("open_indices", openIndices);
        stats.put("closed_indices", closedIndices);
        stats.put("total_documents", totalDocs);
        stats.put("total_size_bytes", totalSize);
        stats.put("total_size_mb", totalSize / (1024.0 * 1024.0));

        return stats;
    }

    // ==================== Utility Methods ====================

    /**
     * Validate index name
     */
    private void validateIndexName(String indexName) throws IndexException {
        if (indexName == null || indexName.trim().isEmpty()) {
            throw new IndexException("Index name cannot be null or empty");
        }

        // Check for invalid characters
        if (!indexName.matches("^[a-zA-Z0-9_-]+$")) {
            throw new IndexException("Index name can only contain letters, numbers, hyphens, and underscores");
        }

        // Check for reserved names
        if (indexName.startsWith(".") || indexName.startsWith("_")) {
            throw new IndexException("Index name cannot start with '.' or '_'");
        }

        // Check length
        if (indexName.length() > 255) {
            throw new IndexException("Index name cannot exceed 255 characters");
        }
    }

    /**
     * Delete a directory recursively
     */
    private void deleteDirectory(Path path) throws IOException {
        if (Files.exists(path)) {
            Files.walk(path)
                    .sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.delete(p);
                        } catch (IOException e) {
                            System.err.println("Failed to delete: " + p);
                        }
                    });
        }
    }

    /**
     * Check if manager is closed
     */
    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("IndexManager is closed");
        }
    }

    // ==================== Lifecycle ====================

    /**
     * Close the IndexManager and all indices
     */
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        globalLock.writeLock().lock();
        try {
            closed = true;
            registry.closeAll();
            instance = null;
        } finally {
            globalLock.writeLock().unlock();
        }
    }

    /**
     * Get a summary of the index manager
     */
    public String getSummary() {
        Map<String, Object> stats = getClusterStats();
        return String.format(
                "IndexManager{basePath='%s', indices=%d (open=%d, closed=%d), docs=%d, size=%.2f MB}",
                basePath, stats.get("total_indices"), stats.get("open_indices"),
                stats.get("closed_indices"), stats.get("total_documents"),
                stats.get("total_size_mb")
        );
    }

    @Override
    public String toString() {
        return getSummary();
    }
}