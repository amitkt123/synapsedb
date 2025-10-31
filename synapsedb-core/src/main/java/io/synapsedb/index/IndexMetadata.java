package io.synapsedb.index;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Metadata information about a SynapseDB index.
 * Tracks creation time, version, mappings, and other index-level information.
 *
 * Metadata about an index (settings, mappings, state)
 * @author Amit Tiwari
 */
public class IndexMetadata {

    private final String indexName;
    private final String indexUUID;
    private final long creationDate;
    private final int version;
    private IndexState state;
    private IndexSettings settings;
    private Map<String, Object> mappings;
    private Map<String, String> aliases;
    private long lastModified;
    private long documentCount;
    private long sizeInBytes;
    private String primaryLocation;
    private Map<String, Object> customMetadata;

    // Constructor for new index
    public IndexMetadata(String indexName, IndexSettings settings) {
        this.indexName = indexName;
        this.indexUUID = UUID.randomUUID().toString();
        this.creationDate = Instant.now().toEpochMilli();
        this.version = 1;
        this.state = IndexState.CREATING;
        this.settings = settings;
        this.mappings = new HashMap<>();
        this.aliases = new HashMap<>();
        this.lastModified = this.creationDate;
        this.documentCount = 0;
        this.sizeInBytes = 0;
        this.customMetadata = new HashMap<>();
    }

    // Constructor for existing index (loaded from disk)
    public IndexMetadata(String indexName, String indexUUID, long creationDate,
                         int version, IndexSettings settings) {
        this.indexName = indexName;
        this.indexUUID = indexUUID;
        this.creationDate = creationDate;
        this.version = version;
        this.state = IndexState.CLOSED; // Will be opened explicitly
        this.settings = settings;
        this.mappings = new HashMap<>();
        this.aliases = new HashMap<>();
        this.lastModified = creationDate;
        this.documentCount = 0;
        this.sizeInBytes = 0;
        this.customMetadata = new HashMap<>();
    }

    /**
     * Update the index state
     */
    public synchronized void setState(IndexState newState) {
        if (this.state == IndexState.DELETING) {
            throw new IllegalStateException("Cannot change state of index being deleted");
        }
        this.state = newState;
        this.lastModified = Instant.now().toEpochMilli();
    }

    /**
     * Update document count
     */
    public synchronized void updateDocumentCount(long count) {
        this.documentCount = count;
        this.lastModified = Instant.now().toEpochMilli();
    }

    /**
     * Update index size
     */
    public synchronized void updateSizeInBytes(long size) {
        this.sizeInBytes = size;
        this.lastModified = Instant.now().toEpochMilli();
    }

    /**
     * Add or update a mapping
     */
    public synchronized void putMapping(String type, Object mapping) {
        this.mappings.put(type, mapping);
        this.lastModified = Instant.now().toEpochMilli();
    }

    /**
     * Add an alias
     */
    public synchronized void addAlias(String alias, String filter) {
        this.aliases.put(alias, filter);
        this.lastModified = Instant.now().toEpochMilli();
    }

    /**
     * Remove an alias
     */
    public synchronized void removeAlias(String alias) {
        this.aliases.remove(alias);
        this.lastModified = Instant.now().toEpochMilli();
    }

    /**
     * Update settings (creates a new version)
     */
    public synchronized void updateSettings(IndexSettings newSettings) {
        this.settings = newSettings;
        this.lastModified = Instant.now().toEpochMilli();
    }

    /**
     * Add custom metadata
     */
    public synchronized void putCustomMetadata(String key, Object value) {
        this.customMetadata.put(key, value);
        this.lastModified = Instant.now().toEpochMilli();
    }

    /**
     * Set primary location (directory path)
     */
    public void setPrimaryLocation(String location) {
        this.primaryLocation = location;
    }

    /**
     * Check if index is in a healthy state
     */
    public boolean isHealthy() {
        return state == IndexState.OPEN || state == IndexState.CLOSED;
    }

    /**
     * Check if index can accept writes
     */
    public boolean canWrite() {
        return state == IndexState.OPEN;
    }

    /**
     * Check if index can be read
     */
    public boolean canRead() {
        return state == IndexState.OPEN;
    }

    /**
     * Get age of index in milliseconds
     */
    public long getAgeInMillis() {
        return Instant.now().toEpochMilli() - creationDate;
    }

    /**
     * Create a snapshot of current metadata
     */
    public IndexMetadata snapshot() {
        IndexMetadata snapshot = new IndexMetadata(indexName, indexUUID,
                creationDate, version, settings.clone());
        snapshot.state = this.state;
        snapshot.mappings = new HashMap<>(this.mappings);
        snapshot.aliases = new HashMap<>(this.aliases);
        snapshot.lastModified = this.lastModified;
        snapshot.documentCount = this.documentCount;
        snapshot.sizeInBytes = this.sizeInBytes;
        snapshot.primaryLocation = this.primaryLocation;
        snapshot.customMetadata = new HashMap<>(this.customMetadata);
        return snapshot;
    }

    // Getters
    public String getIndexName() { return indexName; }
    public String getIndexUUID() { return indexUUID; }
    public long getCreationDate() { return creationDate; }
    public int getVersion() { return version; }
    public IndexState getState() { return state; }
    public IndexSettings getSettings() { return settings; }
    public Map<String, Object> getMappings() { return new HashMap<>(mappings); }
    public Map<String, String> getAliases() { return new HashMap<>(aliases); }
    public long getLastModified() { return lastModified; }
    public long getDocumentCount() { return documentCount; }
    public long getSizeInBytes() { return sizeInBytes; }
    public String getPrimaryLocation() { return primaryLocation; }
    public Map<String, Object> getCustomMetadata() { return new HashMap<>(customMetadata); }

    /**
     * Convert to a Map for serialization
     */
    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("index_name", indexName);
        map.put("index_uuid", indexUUID);
        map.put("creation_date", creationDate);
        map.put("version", version);
        map.put("state", state.toString());
        map.put("settings", settings);
        map.put("mappings", mappings);
        map.put("aliases", aliases);
        map.put("last_modified", lastModified);
        map.put("document_count", documentCount);
        map.put("size_in_bytes", sizeInBytes);
        map.put("primary_location", primaryLocation);
        map.put("custom_metadata", customMetadata);
        return map;
    }

    @Override
    public String toString() {
        return String.format("IndexMetadata{name='%s', uuid='%s', state=%s, docs=%d, size=%d bytes}",
                indexName, indexUUID, state, documentCount, sizeInBytes);
    }
}