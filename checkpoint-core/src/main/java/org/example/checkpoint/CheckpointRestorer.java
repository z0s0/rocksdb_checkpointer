package org.example.checkpoint;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public final class CheckpointRestorer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CheckpointRestorer.class);

    private final CheckpointStore store;
    private final ExecutorService executor;
    private final boolean ownsExecutor;
    private final Metrics metrics;

    public CheckpointRestorer(CheckpointStore store, int parallelism, Metrics metrics) {
        this(store, Executors.newFixedThreadPool(parallelism, namedThreads("ckpt-download")), true, metrics);
    }

    public CheckpointRestorer(CheckpointStore store, ExecutorService executor, Metrics metrics) {
        this(store, executor, false, metrics);
    }

    private CheckpointRestorer(CheckpointStore store, ExecutorService executor, boolean ownsExecutor, Metrics metrics) {
        this.store = Objects.requireNonNull(store, "store");
        this.executor = Objects.requireNonNull(executor, "executor");
        this.ownsExecutor = ownsExecutor;
        this.metrics = metrics != null ? metrics : new Metrics();
    }

    public Manifest restoreLatest(Path target) throws IOException {
        Optional<Manifest> latest = store.getLatestManifest();
        if (latest.isEmpty()) {
            throw new IOException("no manifest available in store");
        }
        return restoreManifest(latest.get(), target);
    }

    public Manifest restore(long version, Path target) throws IOException {
        Optional<Manifest> m = store.getManifest(version);
        if (m.isEmpty()) {
            throw new IOException("manifest v" + version + " not found in store");
        }
        return restoreManifest(m.get(), target);
    }

    public Manifest restoreManifest(Manifest m, Path target) throws IOException {
        Files.createDirectories(target);
        long t0 = System.nanoTime();

        List<CompletableFuture<Void>> futures = new ArrayList<>(m.ssts().size() + m.metaFiles().size());
        for (SstEntry e : m.ssts()) {
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    store.downloadSst(e.name(), target.resolve(e.name()));
                } catch (IOException ex) {
                    throw new UncheckedIOException(ex);
                }
            }, executor));
        }
        for (MetaEntry e : m.metaFiles()) {
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    store.downloadMetaFile(m.version(), e.name(), target.resolve(e.name()));
                } catch (IOException ex) {
                    throw new UncheckedIOException(ex);
                }
            }, executor));
        }
        for (CompletableFuture<Void> f : futures) {
            f.join();
        }

        long totalNs = System.nanoTime() - t0;
        metrics.checkpointRestored(m, totalNs);
        LOG.info("restored manifest v{} ({} ssts, {} meta, {} bytes) in {} ms",
                m.version(), m.ssts().size(), m.metaFiles().size(), m.totalSizeBytes(), totalNs / 1_000_000);
        return m;
    }

    @Override
    public void close() {
        if (ownsExecutor) {
            executor.shutdown();
        }
    }

    private static ThreadFactory namedThreads(String prefix) {
        AtomicInteger n = new AtomicInteger();
        return r -> {
            Thread t = new Thread(r, prefix + "-" + n.getAndIncrement());
            t.setDaemon(true);
            return t;
        };
    }
}
