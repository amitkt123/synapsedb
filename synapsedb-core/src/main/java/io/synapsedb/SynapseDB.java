package io.synapsedb;

import io.synapsedb.collection.Collection;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Main SynapseDB database class with full-text search and aggregation support
 *
 * @author Amit Tiwari
 */
public class SynapseDB {
    private final String name;
    private final Map<String, Collection> collections;
    private volatile boolean closed = false;

    public SynapseDB(String name) {
        this.name = name;
        this.collections = new ConcurrentHashMap<>();
    }

    /**
     * Create or get a collection
     */
    public Collection collection(String collectionName) {
        if (closed) {
            throw new IllegalStateException("Database is closed");
        }
        return collections.computeIfAbsent(collectionName, Collection::new);
    }

    /**
     * Get existing collection (returns null if doesn't exist)
     */
    public Collection getCollection(String collectionName) {
        return collections.get(collectionName);
    }

    /**
     * Drop a collection
     */
    public boolean dropCollection(String collectionName) {
        return collections.remove(collectionName) != null;
    }

    /**
     * List all collection names
     */
    public java.util.Set<String> listCollections() {
        return collections.keySet();
    }

    /**
     * Get database statistics
     */
    public DatabaseStats getStats() {
        long totalDocuments = collections.values().stream()
                .mapToLong(Collection::count)
                .sum();
        return new DatabaseStats(name, collections.size(), totalDocuments);
    }

    /**
     * Close the database
     */
    public void close() {
        closed = true;
        collections.clear();
    }

    /**
     * Check if database is closed
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * Get database name
     */
    public String getName() {
        return name;
    }

    /**
     * Database statistics
     */
    public static class DatabaseStats {
        private final String name;
        private final int collectionCount;
        private final long totalDocuments;

        public DatabaseStats(String name, int collectionCount, long totalDocuments) {
            this.name = name;
            this.collectionCount = collectionCount;
            this.totalDocuments = totalDocuments;
        }

        public String getName() {
            return name;
        }

        public int getCollectionCount() {
            return collectionCount;
        }

        public long getTotalDocuments() {
            return totalDocuments;
        }

        @Override
        public String toString() {
            return String.format("DatabaseStats{name='%s', collections=%d, documents=%d}",
                    name, collectionCount, totalDocuments);
        }
    }
}

