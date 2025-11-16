package io.synapsedb.core.manager;

import io.synapsedb.core.SynapseDB;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

//manager as a double checked singleton
public class SynapseDbManager {
    private static volatile SynapseDbManager INSTANCE;
    private final ConcurrentHashMap<String, SynapseDB> databases;
    private final ReadWriteLock lock;

    private SynapseDbManager() {
        this.lock = new ReentrantReadWriteLock();
        this.databases = new ConcurrentHashMap<>();
    }

    public SynapseDbManager getInstance() {
        if(INSTANCE == null){
            synchronized (SynapseDbManager.class) {
                if(INSTANCE == null){
                    INSTANCE = new SynapseDbManager();
                }
            }
        }
        return INSTANCE;
    }

    public SynapseDB getDatabase(String dbName) {
      return databases.get(dbName);
    }

    public SynapseDB registerDatabase(String dbName){
        return databases.put(dbName, new SynapseDB());
    }

    public void removeDatabase(String dbName){
        databases.remove(dbName);
    }


}
