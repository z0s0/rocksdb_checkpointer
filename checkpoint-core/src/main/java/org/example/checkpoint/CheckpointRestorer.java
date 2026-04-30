package org.example.checkpoint;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
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
    private final BandwidthLimiter bandwidthLimiter;

    public CheckpointRestorer(CheckpointStore store, int parallelism, Metrics metrics) {
        this(store, parallelism, metrics, null);
    }

    public CheckpointRestorer(CheckpointStore store, int parallelism, Metrics metrics, BandwidthLimiter bandwidthLimiter) {
        this(store, Executors.newFixedThreadPool(parallelism, namedThreads("ckpt-download")), true, metrics, bandwidthLimiter);
    }

    public CheckpointRestorer(CheckpointStore store, ExecutorService executor, Metrics metrics) {
        this(store, executor, false, metrics, null);
    }

    public CheckpointRestorer(CheckpointStore store, ExecutorService executor, Metrics metrics, BandwidthLimiter bandwidthLimiter) {
        this(store, executor, false, metrics, bandwidthLimiter);
    }

    private CheckpointRestorer(CheckpointStore store, ExecutorService executor, boolean ownsExecutor,
                               Metrics metrics, BandwidthLimiter bandwidthLimiter) {
        this.store = Objects.requireNonNull(store, "store");
        this.executor = Objects.requireNonNull(executor, "executor");
        this.ownsExecutor = ownsExecutor;
        this.metrics = metrics != null ? metrics : new Metrics();
        this.bandwidthLimiter = bandwidthLimiter;
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
                    acquireBandwidth(e.sizeBytes());
                    store.downloadSst(e.name(), target.resolve(e.name()));
                } catch (IOException ex) {
                    throw new UncheckedIOException(ex);
                }
            }, executor));
        }
        for (MetaEntry e : m.metaFiles()) {
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    acquireBandwidth(e.sizeBytes());
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

    /**
     * Diff-based refresh from {@code currentManifest} to {@code targetManifest}.
     * SSTs reused between the two manifests are hard-linked from {@code liveDir}
     * into {@code nextDir} (no I/O). New SSTs and all meta files for the target
     * version are downloaded into {@code nextDir} in parallel. The caller is
     * responsible for opening RocksDB on {@code nextDir} and disposing of
     * {@code liveDir} after a successful swap.
     *
     * <p>Peak disk while running: live DB + delta. The hard links keep reused
     * SSTs from being copied. If hard-linking is unavailable (cross-filesystem),
     * falls back to a copy with a warning.
     *
     * <p>If a reused SST is missing from {@code liveDir} (e.g. corruption),
     * it is treated as a new SST and downloaded.
     */
    public Manifest refresh(
            Manifest currentManifest,
            Manifest targetManifest,
            Path liveDir,
            Path nextDir
    ) throws IOException {
        Objects.requireNonNull(currentManifest, "currentManifest");
        Objects.requireNonNull(targetManifest, "targetManifest");
        Objects.requireNonNull(liveDir, "liveDir");
        Objects.requireNonNull(nextDir, "nextDir");

        Files.createDirectories(nextDir);
        long t0 = System.nanoTime();

        Set<String> currentSstNames = new HashSet<>();
        for (SstEntry e : currentManifest.ssts()) {
            currentSstNames.add(e.name());
        }

        List<SstEntry> reusedSsts = new ArrayList<>();
        List<SstEntry> newSsts = new ArrayList<>();
        for (SstEntry e : targetManifest.ssts()) {
            if (currentSstNames.contains(e.name())) {
                reusedSsts.add(e);
            } else {
                newSsts.add(e);
            }
        }

        int linkOk = 0;
        int linkFallback = 0;
        for (SstEntry e : reusedSsts) {
            Path src = liveDir.resolve(e.name());
            Path dst = nextDir.resolve(e.name());
            if (!Files.exists(src)) {
                LOG.warn("reused SST {} missing from liveDir, will download", e.name());
                newSsts.add(e);
                continue;
            }
            try {
                Files.createLink(dst, src);
                linkOk++;
            } catch (IOException | UnsupportedOperationException linkEx) {
                LOG.warn("hard-link failed for {} ({}); falling back to copy", e.name(), linkEx.toString());
                Files.copy(src, dst);
                linkFallback++;
            }
        }

        List<CompletableFuture<Void>> futures = new ArrayList<>(newSsts.size() + targetManifest.metaFiles().size());
        for (SstEntry e : newSsts) {
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    acquireBandwidth(e.sizeBytes());
                    store.downloadSst(e.name(), nextDir.resolve(e.name()));
                } catch (IOException ex) {
                    throw new UncheckedIOException(ex);
                }
            }, executor));
        }
        for (MetaEntry e : targetManifest.metaFiles()) {
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    acquireBandwidth(e.sizeBytes());
                    store.downloadMetaFile(targetManifest.version(), e.name(), nextDir.resolve(e.name()));
                } catch (IOException ex) {
                    throw new UncheckedIOException(ex);
                }
            }, executor));
        }
        for (CompletableFuture<Void> f : futures) {
            f.join();
        }

        long bytesNew = 0;
        for (SstEntry e : newSsts) {
            bytesNew += e.sizeBytes();
        }

        long totalNs = System.nanoTime() - t0;
        int sstReused = linkOk + linkFallback;
        metrics.checkpointRefreshed(
                targetManifest,
                currentManifest.version(),
                totalNs,
                newSsts.size(),
                sstReused,
                linkOk,
                linkFallback,
                bytesNew
        );
        LOG.info("refreshed v{} -> v{} in {} ms (sstNew={}, sstReused={}, linkOk={}, linkFallback={}, bytesNew={})",
                currentManifest.version(), targetManifest.version(), totalNs / 1_000_000,
                newSsts.size(), sstReused, linkOk, linkFallback, bytesNew);
        return targetManifest;
    }

    @Override
    public void close() {
        if (ownsExecutor) {
            executor.shutdown();
        }
    }

    private void acquireBandwidth(long bytes) {
        if (bandwidthLimiter == null || bytes <= 0) return;
        try {
            bandwidthLimiter.acquire(bytes);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("interrupted while waiting for bandwidth budget", ie);
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
