package org.example.server;

import org.example.checkpoint.Manifest;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Holds the live (RocksDB, Manifest, dataDir) tuple under a read/write lock.
 * Reads (lookups, manifest queries) acquire the read lock. Refresh swaps the
 * tuple atomically under the write lock and closes the previous DB.
 *
 * <p>Read latency added by the lock: a single uncontended read-lock acquire
 * is on the order of nanoseconds. The write lock is held only during a swap
 * (close old DB; takes single-digit ms typically) so ongoing reads block
 * briefly during a refresh and resume immediately after.
 */
public final class DbHandle implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DbHandle.class);

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private RocksDB db;
    private Manifest manifest;
    private Path currentDir;

    public DbHandle(RocksDB db, Manifest manifest, Path currentDir) {
        this.db = db;
        this.manifest = manifest;
        this.currentDir = currentDir;
    }

    public byte[] get(byte[] key) throws RocksDBException {
        lock.readLock().lock();
        try {
            return db.get(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    public Manifest manifest() {
        lock.readLock().lock();
        try {
            return manifest;
        } finally {
            lock.readLock().unlock();
        }
    }

    public Path currentDir() {
        lock.readLock().lock();
        try {
            return currentDir;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Atomically install a new (db, manifest, currentDir) tuple, closing the
     * previous DB under the write lock. Returns the previous {@code currentDir}
     * so the caller can delete it (after a successful swap, the old dir is no
     * longer referenced).
     */
    public Path swap(RocksDB newDb, Manifest newManifest, Path newDir) {
        lock.writeLock().lock();
        try {
            RocksDB oldDb = this.db;
            Path oldDir = this.currentDir;
            this.db = newDb;
            this.manifest = newManifest;
            this.currentDir = newDir;
            try {
                oldDb.close();
            } catch (Exception e) {
                LOG.warn("error closing previous DB", e);
            }
            return oldDir;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() {
        lock.writeLock().lock();
        try {
            try { db.close(); } catch (Exception e) { LOG.warn("close failed", e); }
        } finally {
            lock.writeLock().unlock();
        }
    }
}
