package io.synapsedb.core.manager;

import io.synapsedb.core.SynapseDB;
import io.synapsedb.core.exception.DbManagerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Manages the lifecycle and access to SynapseDB instances within the application.
 * Implemented as a thread-safe singleton.
 */
public final class SynapseDbManager implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(SynapseDbManager.class);
    // Use an Inner Static Class for robust, thread-safe lazy initialization of the singleton instance
    private static class LazyHolder {
        static final SynapseDbManager INSTANCE = new SynapseDbManager();
    }

    // Use AtomicBoolean for thread-safe state management of closure status
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final ConcurrentHashMap<String, SynapseDB> databases = new ConcurrentHashMap<>();
    // ReadWriteLock is generally not necessary here given ConcurrentHashMap's guarantees for basic operations,
    // but kept as it was in the original code, primarily for potential complex, multi-step operations that need synchronization.
    private final ReadWriteLock lock = new ReentrantReadWriteLock();


    /**
     * Private constructor to enforce the singleton pattern.
     */
    private SynapseDbManager() {
        logger.info("SynapseDbManager initialized.");
    }

    /**
     * Provides the global access point to the SynapseDbManager instance.
     * @return The singleton instance of SynapseDbManager.
     */
    public static SynapseDbManager getInstance() {
        return LazyHolder.INSTANCE;
    }

    /**
     * Retrieves a database instance by name.
     * @param dbName The name of the database.
     * @return The SynapseDB instance, or null if not found.
     */
    public SynapseDB getDatabase(String dbName) {
        if (isClosed.get()) {
            throw new IllegalStateException("Cannot access databases. The manager is closed.");
        }
        return databases.get(dbName);
    }

    /**
     * Registers and stores a new database instance.
     * Throws an exception if the manager is already closed.
     * @param dbName The name of the database to register.
     * @param database The SynapseDB instance to register.
     * @throws IllegalStateException if the manager is closed.
     * @return The previously registered database instance, or null if none was present.
     */
    public SynapseDB registerDatabase(String dbName, SynapseDB database) {
        if (isClosed.get()) {
            throw new IllegalStateException("Cannot register database. The manager is closed.");
        }
        // Validate input early
        if (dbName == null || dbName.trim().isEmpty() || database == null) {
            throw new IllegalArgumentException("Database name and instance cannot be null or empty.");
        }
        return databases.put(dbName, database);
    }

    /**
     * Removes a database instance from management.
     * @param dbName The name of the database to remove.
     */
    public void removeDatabase(String dbName) {
        databases.remove(dbName);
    }

    /**
     * Checks if the manager has been shut down.
     * @return true if closed, false otherwise.
     */
    public boolean isClosed() {
        return isClosed.get();
    }

    /**
     * Returns an unmodifiable map of currently managed databases.
     * @return A map of databases.
     */
    public Map<String, SynapseDB> getDatabases() {
        return Collections.unmodifiableMap(databases);
    }

    /**
     * Initiates a safe shutdown of the manager and all managed databases.
     * This method is idempotent and handles potential closure errors gracefully.
     * It uses the AutoCloseable interface standard method name 'close()' instead of 'shutdownDbManager()'.
     */
    @Override
    public void close() {
        // Use compareAndSet to ensure only one thread performs the shutdown logic
        if (!isClosed.compareAndSet(false, true)) {
            logger.info("DB Manager is already closed or shutting down.");
            return;
        }

        logger.info("Attempting to close all connections to databases...");

        for (SynapseDB db : databases.values()) {
            try {
                // Assuming SynapseDB implements AutoCloseable and its close method is idempotent and safe
                db.close();
                logger.debug("Successfully ensured closure of database connection: {}", db.getName());
            } catch (Exception e) {
                // Log the error but continue shutting down other databases (robustness)
                logger.error("Error closing the database connection {}. Continuing with others.", db.getName(), e);
                // We do not rethrow the exception here; a full shutdown should proceed.
            }
        }
        databases.clear();
        logger.info("DB Manager successfully shut down.");
    }
}
