package io.synapsedb.index;

import io.synapsedb.document.Document;
import io.synapsedb.document.mapper.DocumentConverter;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;

import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Core index abstraction that manages a single Lucene index.
 * Handles IndexWriter lifecycle, refresh/flush operations, and provides thread-safe access.
 * Represents an index (wrapper around Lucene Directory, writer, readers etc.)
 * @author Amit Tiwari
 */
public class Index implements Closeable {

    private final String indexName;
    private final IndexMetadata metadata;
    private final IndexSettings settings;
    private final IndexStats stats;
    private final Path indexPath;

    // Lucene components
    private Directory directory;
    private IndexWriter indexWriter;
    private SearcherManager searcherManager;
    private Analyzer analyzer;

    // State management
    private final AtomicReference<IndexState> state;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final ReadWriteLock stateLock = new ReentrantReadWriteLock();

    // Background tasks
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> refreshTask;
    private ScheduledFuture<?> commitTask;

    // Constants
    private static final int SEARCHER_MANAGER_REFRESH_INTERVAL_MS = 100;

    /**
     * Create a new index
     */
    public Index(String indexName, Path basePath, IndexSettings settings) throws IOException, IndexCreationException {
        this.indexName = indexName;
        this.settings = settings != null ? settings : IndexSettings.defaultSettings();
        this.metadata = new IndexMetadata(indexName, this.settings);
        this.stats = new IndexStats(indexName);
        this.indexPath = basePath.resolve(indexName);
        this.state = new AtomicReference<>(IndexState.CREATING);

        initialize();
    }

    /**
     * Open an existing index
     */
    public static Index open(String indexName, Path basePath, IndexMetadata metadata) throws IOException, IndexCreationException {
        Index index = new Index(indexName, basePath, metadata.getSettings());
        index.metadata.setState(IndexState.OPEN);
        index.state.set(IndexState.OPEN);
        return index;
    }

    /**
     * Initialize the index
     */
    private void initialize() throws IOException, IndexCreationException {
        try (LockGuard ignored = LockGuard.of(stateLock.writeLock())) {
            // Create directory
            this.directory = createDirectory(indexPath);

            // Create analyzer
            this.analyzer = createAnalyzer();

            // Create IndexWriter
            IndexWriterConfig config = createIndexWriterConfig();
            this.indexWriter = new IndexWriter(directory, config);

            // Create SearcherManager
            this.searcherManager = new SearcherManager(indexWriter, true, false, new SearcherFactory());

            // Initialize background tasks
            this.scheduler = Executors.newScheduledThreadPool(2, new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, "index-" + indexName + "-scheduler");
                    t.setDaemon(true);
                    return t;
                }
            });

            // Schedule refresh task if auto-refresh is enabled
            if (settings.isAutoRefresh()) {
                scheduleRefreshTask();
            }

            // Schedule commit task if auto-commit is enabled
            if (settings.isAutoCommit()) {
                scheduleCommitTask();
            }

            // Update state
            state.set(IndexState.OPEN);
            metadata.setState(IndexState.OPEN);
            metadata.setPrimaryLocation(indexPath.toString());

        } catch (IOException e) {
            state.set(IndexState.FAILED);
            metadata.setState(IndexState.FAILED);
            cleanup();
            throw new IndexCreationException(indexName, e);
        }
    }

    // Auto-closeable lock helper
    private static class LockGuard implements AutoCloseable {
        private final Lock lock;

        private LockGuard(Lock lock) {
            this.lock = lock;
            this.lock.lock();
        }

        public static LockGuard of(Lock lock) {
            return new LockGuard(lock);
        }

        @Override
        public void close() {
            lock.unlock();
        }
    }

    /**
     * Create appropriate directory based on OS and settings
     */
    private Directory createDirectory(Path path) throws IOException {
        // Create parent directories if needed
        path.toFile().mkdirs();

        // Use MMapDirectory for better performance on 64-bit systems
        if (System.getProperty("os.arch").contains("64")) {
            return new MMapDirectory(path);
        } else {
            // Fallback to NIOFSDirectory
            return NIOFSDirectory.open(path);
        }
    }

    /**
     * Create analyzer (can be customized later)
     */
    private Analyzer createAnalyzer() {
        // TODO: Support custom analyzers from settings
        return new StandardAnalyzer();
    }

    /**
     * Create IndexWriter configuration
     */
    private IndexWriterConfig createIndexWriterConfig() {
        IndexWriterConfig config = new IndexWriterConfig(analyzer);

        // RAM buffer settings
        config.setRAMBufferSizeMB(settings.getRamBufferSizeMB());
        if (settings.getMaxBufferedDocs() > 0) {
            config.setMaxBufferedDocs(settings.getMaxBufferedDocs());
        }

        // Merge policy
        config.setMergePolicy(settings.createMergePolicy());

        // Other settings
        config.setUseCompoundFile(settings.isUseCompoundFile());
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

        return config;
    }

    /**
     * Schedule periodic refresh
     */
    private void scheduleRefreshTask() {
        long interval = settings.getRefreshIntervalMs();
        refreshTask = scheduler.scheduleWithFixedDelay(() -> {
            try {
                refresh();
            } catch (Exception e) {
                // Log error but don't stop the task
                System.err.println("Error during auto-refresh: " + e.getMessage());
            }
        }, interval, interval, TimeUnit.MILLISECONDS);
    }

    /**
     * Schedule periodic commit
     */
    private void scheduleCommitTask() {
        long interval = settings.getCommitIntervalMs();
        commitTask = scheduler.scheduleWithFixedDelay(() -> {
            try {
                commit();
            } catch (Exception e) {
                // Log error but don't stop the task
                System.err.println("Error during auto-commit: " + e.getMessage());
            }
        }, interval, interval, TimeUnit.MILLISECONDS);
    }

    // ==================== Document Operations ====================

    /**
     * Add a document to the index
     */
    public void addDocument(Document document) throws IOException, InvalidIndexStateException {
        checkWriteable();

        long startTime = System.currentTimeMillis();
        stats.incrementIndexingCurrent();

        try {
            org.apache.lucene.document.Document luceneDoc = DocumentConverter.toLuceneDocument(document);
            indexWriter.addDocument(luceneDoc);

            long elapsed = System.currentTimeMillis() - startTime;
            stats.recordIndexing(elapsed, true);

        } catch (Exception e) {
            long elapsed = System.currentTimeMillis() - startTime;
            stats.recordIndexing(elapsed, false);
            throw new IOException("Failed to add document: " + e.getMessage(), e);
        } finally {
            stats.decrementIndexingCurrent();
        }
    }

    /**
     * Update a document in the index
     */
    public void updateDocument(String id, Document document) throws IOException, InvalidIndexStateException {
        checkWriteable();

        long startTime = System.currentTimeMillis();
        stats.incrementIndexingCurrent();

        try {
            Term idTerm = new Term("_id", id);
            org.apache.lucene.document.Document luceneDoc = DocumentConverter.toLuceneDocument(document);
            indexWriter.updateDocument(idTerm, luceneDoc);

            long elapsed = System.currentTimeMillis() - startTime;
            stats.recordIndexing(elapsed, true);

        } catch (Exception e) {
            long elapsed = System.currentTimeMillis() - startTime;
            stats.recordIndexing(elapsed, false);
            throw new IOException("Failed to update document: " + e.getMessage(), e);
        } finally {
            stats.decrementIndexingCurrent();
        }
    }

    /**
     * Delete a document from the index
     */
    public void deleteDocument(String id) throws IOException, InvalidIndexStateException {
        checkWriteable();

        Term idTerm = new Term("_id", id);
        indexWriter.deleteDocuments(idTerm);
    }

    /**
     * Delete documents matching a query
     */
    public void deleteDocuments(Query... queries) throws IOException, InvalidIndexStateException {
        checkWriteable();
        indexWriter.deleteDocuments(queries);
    }

    /**
     * Delete all documents in the index
     */
    public void deleteAll() throws IOException, InvalidIndexStateException {
        checkWriteable();
        indexWriter.deleteAll();
    }

    // ==================== Index Operations ====================

    /**
     * Refresh the index to make recent changes searchable
     */
    public void refresh() throws IOException {
        if (state.get() != IndexState.OPEN) {
            return; // Skip refresh if not open
        }

        long startTime = System.currentTimeMillis();

        try {
            searcherManager.maybeRefresh();
            long elapsed = System.currentTimeMillis() - startTime;
            stats.recordRefresh(elapsed);
        } catch (IOException e) {
            throw new IOException("Failed to refresh index: " + e.getMessage(), e);
        }
    }

    /**
     * Commit changes to disk
     */
    public void commit() throws IOException, InvalidIndexStateException {
        checkWriteable();

        indexWriter.commit();
        updateStats();
    }

    /**
     * Flush changes to disk (without committing)
     */
    public void flush() throws IOException, InvalidIndexStateException {
        checkWriteable();

        long startTime = System.currentTimeMillis();

        try {
            indexWriter.flush();
            long elapsed = System.currentTimeMillis() - startTime;
            stats.recordFlush(elapsed);
        } catch (IOException e) {
            throw new IOException("Failed to flush index: " + e.getMessage(), e);
        }
    }

    /**
     * Force merge segments
     */
    public void forceMerge(int maxNumSegments) throws IOException, InvalidIndexStateException {
        checkWriteable();

        long startTime = System.currentTimeMillis();
        stats.incrementMergeCurrent();

        try {
            indexWriter.forceMerge(maxNumSegments);
            long elapsed = System.currentTimeMillis() - startTime;
            stats.recordMerge(elapsed, 0); // Size calculation would need segment info
        } finally {
            stats.decrementMergeCurrent();
        }
    }

    /**
     * Get an IndexSearcher for searching
     */
    public IndexSearcher acquireSearcher() throws IOException, InvalidIndexStateException {
        if (state.get() != IndexState.OPEN) {
            throw new InvalidIndexStateException(indexName, state.get(), "search");
        }

        stats.incrementSearchCurrent();
        return searcherManager.acquire();
    }

    /**
     * Release a searcher after use
     */
    public void releaseSearcher(IndexSearcher searcher) throws IOException {
        searcherManager.release(searcher);
        stats.decrementSearchCurrent();
    }

    // ==================== State Management ====================

    /**
     * Close the index
     */
    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            stateLock.writeLock().lock();
            try {
                state.set(IndexState.CLOSED);
                metadata.setState(IndexState.CLOSED);

                // Cancel scheduled tasks
                if (refreshTask != null) {
                    refreshTask.cancel(false);
                }
                if (commitTask != null) {
                    commitTask.cancel(false);
                }

                // Shutdown scheduler
                if (scheduler != null) {
                    scheduler.shutdown();
                    try {
                        if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                            scheduler.shutdownNow();
                        }
                    } catch (InterruptedException e) {
                        scheduler.shutdownNow();
                        Thread.currentThread().interrupt();
                    }
                }

                // Close Lucene components
                if (searcherManager != null) {
                    searcherManager.close();
                }
                if (indexWriter != null && indexWriter.isOpen()) {
                    indexWriter.close();
                }
                if (directory != null) {
                    directory.close();
                }

            } finally {
                stateLock.writeLock().unlock();
            }
        }
    }

    /**
     * Reopen a closed index
     */
    public void open() throws IOException, InvalidIndexStateException {
        if (state.get() == IndexState.CLOSED) {
            stateLock.writeLock().lock();
            try {
                if (closed.get()) {
                    closed.set(false);
                    initialize();
                }
            } catch (IndexCreationException e) {
                throw new RuntimeException(e);
            } finally {
                stateLock.writeLock().unlock();
            }
        } else {
            throw new InvalidIndexStateException(indexName, state.get(), "open");
        }
    }

    /**
     * Check if index is writeable
     */
    private void checkWriteable() throws InvalidIndexStateException {
        IndexState currentState = state.get();
        if (!currentState.isWriteable()) {
            throw new InvalidIndexStateException(indexName, currentState, "write");
        }
    }

    /**
     * Update index statistics
     */
    private void updateStats() {
        try {
            DirectoryReader reader = DirectoryReader.open(indexWriter);
            stats.updateDocumentStats(reader.numDocs(), reader.numDeletedDocs(), reader.maxDoc());

            // Update metadata
            metadata.updateDocumentCount(reader.numDocs());

            // Calculate size (approximate)
            long size = 0;
            for (String file : directory.listAll()) {
                size += directory.fileLength(file);
            }
            stats.updateStorageStats(size, 0); // Translog not implemented yet
            metadata.updateSizeInBytes(size);

            reader.close();
        } catch (IOException e) {
            // Log error but don't fail
            System.err.println("Error updating stats: " + e.getMessage());
        }
    }

    /**
     * Cleanup resources on failure
     */
    private void cleanup() {
        try {
            close();
        } catch (Exception e) {
            // Ignore errors during cleanup
        }
    }

    // ==================== Getters ====================

    public String getIndexName() { return indexName; }
    public IndexMetadata getMetadata() { return metadata; }
    public IndexSettings getSettings() { return settings; }
    public IndexStats getStats() { return stats; }
    public IndexState getState() { return state.get(); }
    public Path getIndexPath() { return indexPath; }
    public boolean isClosed() { return closed.get(); }

    @Override
    public String toString() {
        return String.format("Index{name='%s', state=%s, path='%s', docs=%d}",
                indexName, state.get(), indexPath, stats.getTotalDocs());
    }
}
