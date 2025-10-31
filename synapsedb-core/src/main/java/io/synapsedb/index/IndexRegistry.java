package io.synapsedb.index;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Central registry for managing multiple indices.
 * Thread-safe repository that tracks all indices in the system.
 */
public class IndexRegistry {

    private final Path basePath;
    private final Map<String, Index> indices;
    private final Map<String, Set<String>> aliasToIndices;
    private final Map<String, IndexMetadata> metadataCache;

    public IndexRegistry(Path basePath) {
        this.basePath = basePath;
        this.indices = new ConcurrentHashMap<>();
        this.aliasToIndices = new ConcurrentHashMap<>();
        this.metadataCache = new ConcurrentHashMap<>();

        // Create base directory if it doesn't exist
        try {
            Files.createDirectories(basePath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create base directory: " + basePath, e);
        }
    }

    /**
     * Register a new index
     */
    public synchronized void registerIndex(Index index) throws IndexAlreadyExistsException {
        String indexName = index.getIndexName();

        if (indices.containsKey(indexName)) {
            throw new IndexAlreadyExistsException(indexName);
        }

        indices.put(indexName, index);
        metadataCache.put(indexName, index.getMetadata());

        // Register any aliases
        for (Map.Entry<String, String> alias : index.getMetadata().getAliases().entrySet()) {
            addAliasMapping(alias.getKey(), indexName);
        }
    }

    /**
     * Unregister an index
     */
    public synchronized void unregisterIndex(String indexName) {
        Index index = indices.remove(indexName);
        if (index != null) {
            metadataCache.remove(indexName);

            // Remove from aliases
            for (Map.Entry<String, Set<String>> entry : aliasToIndices.entrySet()) {
                entry.getValue().remove(indexName);
            }

            // Clean up empty alias entries
            aliasToIndices.entrySet().removeIf(entry -> entry.getValue().isEmpty());
        }
    }

    /**
     * Get an index by name
     */
    public Index getIndex(String indexName) throws IndexNotFoundException {
        Index index = indices.get(indexName);
        if (index == null) {
            // Check if it's an alias
            Set<String> aliasedIndices = aliasToIndices.get(indexName);
            if (aliasedIndices != null && !aliasedIndices.isEmpty()) {
                // Return the first index (for single alias)
                // TODO: Handle multiple indices per alias
                String actualIndexName = aliasedIndices.iterator().next();
                index = indices.get(actualIndexName);
            }
        }

        if (index == null) {
            throw new IndexNotFoundException(indexName);
        }

        return index;
    }

    /**
     * Check if an index exists
     */
    public boolean indexExists(String indexName) {
        return indices.containsKey(indexName) || aliasToIndices.containsKey(indexName);
    }

    /**
     * Get all index names
     */
    public Set<String> getIndexNames() {
        return new HashSet<>(indices.keySet());
    }

    /**
     * Get all indices
     */
    public Collection<Index> getAllIndices() {
        return new ArrayList<>(indices.values());
    }

    /**
     * Get indices matching a pattern
     */
    public List<Index> getIndicesByPattern(String pattern) {
        String regex = pattern.replace("*", ".*");
        return indices.entrySet().stream()
                .filter(entry -> entry.getKey().matches(regex))
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
    }

    /**
     * Get metadata for an index
     */
    public IndexMetadata getIndexMetadata(String indexName) throws IndexNotFoundException {
        IndexMetadata metadata = metadataCache.get(indexName);
        if (metadata == null) {
            throw new IndexNotFoundException(indexName);
        }
        return metadata;
    }

    /**
     * Get all index metadata
     */
    public Map<String, IndexMetadata> getAllMetadata() {
        return new HashMap<>(metadataCache);
    }

    // ==================== Alias Management ====================

    /**
     * Add an alias to an index
     */
    public synchronized void addAlias(String alias, String indexName) throws IndexNotFoundException {
        if (!indices.containsKey(indexName)) {
            throw new IndexNotFoundException(indexName);
        }

        addAliasMapping(alias, indexName);

        // Update index metadata
        Index index = indices.get(indexName);
        index.getMetadata().addAlias(alias, null);
    }

    /**
     * Remove an alias
     */
    public synchronized void removeAlias(String alias, String indexName) throws IndexNotFoundException {
        if (!indices.containsKey(indexName)) {
            throw new IndexNotFoundException(indexName);
        }

        Set<String> indexSet = aliasToIndices.get(alias);
        if (indexSet != null) {
            indexSet.remove(indexName);
            if (indexSet.isEmpty()) {
                aliasToIndices.remove(alias);
            }
        }

        // Update index metadata
        Index index = indices.get(indexName);
        index.getMetadata().removeAlias(alias);
    }

    /**
     * Get all aliases
     */
    public Map<String, Set<String>> getAllAliases() {
        return new HashMap<>(aliasToIndices);
    }

    /**
     * Get indices for an alias
     */
    public Set<String> getIndicesForAlias(String alias) {
        Set<String> indices = aliasToIndices.get(alias);
        return indices != null ? new HashSet<>(indices) : Collections.emptySet();
    }

    /**
     * Swap an alias from one index to another atomically
     */
    public synchronized void swapAlias(String alias, String fromIndex, String toIndex)
            throws IndexNotFoundException {
        if (!indices.containsKey(fromIndex)) {
            throw new IndexNotFoundException(fromIndex);
        }
        if (!indices.containsKey(toIndex)) {
            throw new IndexNotFoundException(toIndex);
        }

        // Remove from old index
        removeAlias(alias, fromIndex);

        // Add to new index
        addAlias(alias, toIndex);
    }

    /**
     * Internal method to add alias mapping
     */
    private void addAliasMapping(String alias, String indexName) {
        aliasToIndices.computeIfAbsent(alias, k -> ConcurrentHashMap.newKeySet()).add(indexName);
    }

    // ==================== Discovery ====================

    /**
     * Discover existing indices from disk
     */
    public synchronized void discoverIndices() throws IOException {
        if (!Files.exists(basePath)) {
            return;
        }

        try (Stream<Path> paths = Files.list(basePath)) {
            List<Path> indexDirs = paths
                    .filter(Files::isDirectory)
                    .filter(path -> !path.getFileName().toString().startsWith("."))
                    .collect(Collectors.toList());

            for (Path indexDir : indexDirs) {
                String indexName = indexDir.getFileName().toString();

                if (!indices.containsKey(indexName)) {
                    try {
                        // Try to load index metadata
                        IndexMetadata metadata = loadIndexMetadata(indexDir);

                        if (metadata != null) {
                            // Open the existing index
                            Index index = Index.open(indexName, basePath, metadata);
                            registerIndex(index);
                        }
                    } catch (Exception e) {
                        // Log error but continue with other indices
                        System.err.println("Failed to load index " + indexName + ": " + e.getMessage());
                    }
                }
            }
        }
    }

    /**
     * Load index metadata from disk
     */
    private IndexMetadata loadIndexMetadata(Path indexPath) throws IOException {
        // TODO: Implement metadata persistence
        // For now, create default metadata
        String indexName = indexPath.getFileName().toString();
        return new IndexMetadata(indexName, IndexSettings.defaultSettings());
    }

    // ==================== Cleanup ====================

    /**
     * Close all indices
     */
    public synchronized void closeAll() throws IOException {
        List<IOException> exceptions = new ArrayList<>();

        for (Index index : indices.values()) {
            try {
                index.close();
            } catch (IOException e) {
                exceptions.add(e);
            }
        }

        if (!exceptions.isEmpty()) {
            IOException combined = new IOException("Failed to close some indices");
            exceptions.forEach(combined::addSuppressed);
            throw combined;
        }
    }

    /**
     * Get registry statistics
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("total_indices", indices.size());
        stats.put("total_aliases", aliasToIndices.size());
        stats.put("base_path", basePath.toString());

        // Aggregate stats from all indices
        long totalDocs = 0;
        long totalSize = 0;

        for (Index index : indices.values()) {
            IndexStats indexStats = index.getStats();
            totalDocs += indexStats.getTotalDocs();
            totalSize += indexStats.getStoreSizeInBytes();
        }

        stats.put("total_documents", totalDocs);
        stats.put("total_size_bytes", totalSize);

        return stats;
    }

    @Override
    public String toString() {
        return String.format("IndexRegistry{basePath='%s', indices=%d, aliases=%d}",
                basePath, indices.size(), aliasToIndices.size());
    }
}