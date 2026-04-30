package org.example.server;

import org.example.checkpoint.CheckpointRestorer;
import org.example.checkpoint.CheckpointStore;
import org.example.checkpoint.Manifest;
import org.example.checkpoint.ShardAssignment;
import org.example.checkpoint.SstEntry;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

/**
 * Single-flight diff-based refresh of the live DbHandle to a target manifest.
 *
 * <p>Workflow:
 * <ol>
 *   <li>Resolve target manifest (latest by default, or pinned by version).</li>
 *   <li>Validate shard ownership.</li>
 *   <li>Stage download into {@code restoreParent/v<targetVersion>/} via
 *       {@link CheckpointRestorer#refresh}: hard-links reused SSTs from the
 *       live dir, downloads only new SSTs and meta files.</li>
 *   <li>Open new RocksDB on the staged dir.</li>
 *   <li>Atomically swap the new tuple into {@link DbHandle} (closes old DB).</li>
 *   <li>Schedule background deletion of the old dir.</li>
 * </ol>
 *
 * <p>Concurrency: a {@link ReentrantLock} guarantees single-flight; concurrent
 * callers receive {@link RefreshInProgressException}.
 */
public final class RefreshController implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RefreshController.class);

    private final DbHandle handle;
    private final CheckpointStore store;
    private final CheckpointRestorer restorer;
    private final Options dbOptions;
    private final Path restoreParent;
    private final ShardAssignment shard;
    private final ReentrantLock refreshLock = new ReentrantLock();
    private final ExecutorService cleanupExecutor;

    public RefreshController(
            DbHandle handle,
            CheckpointStore store,
            CheckpointRestorer restorer,
            Options dbOptions,
            Path restoreParent,
            ShardAssignment shard
    ) {
        this.handle = handle;
        this.store = store;
        this.restorer = restorer;
        this.dbOptions = dbOptions;
        this.restoreParent = restoreParent;
        this.shard = shard;
        this.cleanupExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "restore-cleanup");
            t.setDaemon(true);
            return t;
        });
    }

    public RefreshResult refresh(long versionOrLatest) throws Exception {
        if (!refreshLock.tryLock()) {
            throw new RefreshInProgressException();
        }
        try {
            Manifest current = handle.manifest();
            Manifest target = resolveTarget(versionOrLatest);

            if (target.version() == current.version()) {
                LOG.info("refresh no-op: already at v{}", current.version());
                return new RefreshResult(target, current.version(), 0, 0, true);
            }
            if (target.version() < current.version()) {
                throw new IllegalStateException("refresh target v" + target.version()
                        + " is older than current v" + current.version());
            }

            shard.assertOwns(target);

            Path nextDir = restoreParent.resolve("v" + target.version());
            // If a previous refresh aborted partway, clean stale staging dir for the same version.
            deleteRecursively(nextDir);
            Files.createDirectories(nextDir);

            Path liveDir = handle.currentDir();
            restorer.refresh(current, target, liveDir, nextDir);

            RocksDB newDb = RocksDB.open(dbOptions, nextDir.toString());
            Path oldDir = handle.swap(newDb, target, nextDir);

            cleanupExecutor.submit(() -> {
                try {
                    deleteRecursively(oldDir);
                    LOG.info("deleted previous restore dir {}", oldDir);
                } catch (Exception e) {
                    LOG.warn("failed to delete previous restore dir {}", oldDir, e);
                }
            });

            int sstNew = countNewSsts(current, target);
            int sstReused = target.ssts().size() - sstNew;
            return new RefreshResult(target, current.version(), sstNew, sstReused, false);
        } finally {
            refreshLock.unlock();
        }
    }

    private Manifest resolveTarget(long versionOrLatest) throws IOException {
        if (versionOrLatest < 0) {
            return store.getLatestManifest()
                    .orElseThrow(() -> new IllegalStateException(
                            "no manifest available in store for " + shard.describe()));
        }
        return store.getManifest(versionOrLatest)
                .orElseThrow(() -> new IllegalStateException(
                        "manifest v" + versionOrLatest + " not found in store"));
    }

    private static int countNewSsts(Manifest current, Manifest target) {
        Set<String> have = new HashSet<>();
        for (SstEntry e : current.ssts()) have.add(e.name());
        int n = 0;
        for (SstEntry e : target.ssts()) if (!have.contains(e.name())) n++;
        return n;
    }

    @Override
    public void close() {
        cleanupExecutor.shutdown();
    }

    private static void deleteRecursively(Path p) throws IOException {
        if (!Files.exists(p)) return;
        try (Stream<Path> s = Files.walk(p)) {
            s.sorted(Comparator.reverseOrder()).forEach(x -> {
                try { Files.deleteIfExists(x); } catch (IOException e) { throw new UncheckedIOException(e); }
            });
        }
    }

    public record RefreshResult(Manifest manifest, long fromVersion, int sstNew, int sstReused, boolean noOp) {}

    public static final class RefreshInProgressException extends RuntimeException {
        public RefreshInProgressException() {
            super("a refresh is already in progress");
        }
    }
}
