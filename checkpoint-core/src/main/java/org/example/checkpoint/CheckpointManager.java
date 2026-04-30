package org.example.checkpoint;

import org.rocksdb.Checkpoint;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public final class CheckpointManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CheckpointManager.class);
    private static final String LOCK_FILE = "LOCK";

    private final RocksDB db;
    private final Path stagingRoot;
    private final CheckpointStore store;
    private final OffsetSupplier offsetSupplier;
    private final ExecutorService uploadExecutor;
    private final boolean ownsExecutor;
    private final boolean computeChecksums;
    private final int retainLastN;
    private final Metrics metrics;
    private final BandwidthLimiter uploadBandwidthLimiter;

    private Manifest prevManifest;

    private CheckpointManager(Builder b) throws IOException {
        this.db = Objects.requireNonNull(b.db, "db");
        this.stagingRoot = Objects.requireNonNull(b.stagingRoot, "stagingRoot");
        this.store = Objects.requireNonNull(b.store, "store");
        this.offsetSupplier = Objects.requireNonNull(b.offsetSupplier, "offsetSupplier");
        this.computeChecksums = b.computeChecksums;
        this.retainLastN = b.retainLastN;
        this.metrics = b.metrics != null ? b.metrics : new Metrics();
        this.uploadBandwidthLimiter = b.uploadBandwidthLimiter;
        if (b.uploadExecutor != null) {
            this.uploadExecutor = b.uploadExecutor;
            this.ownsExecutor = false;
        } else {
            this.uploadExecutor = Executors.newFixedThreadPool(b.uploadParallelism, namedThreads("ckpt-upload"));
            this.ownsExecutor = true;
        }
        Files.createDirectories(stagingRoot);
        this.prevManifest = store.getLatestManifest().orElse(null);
        if (prevManifest != null) {
            LOG.info("resuming from manifest v{} ({} ssts, {} bytes)",
                    prevManifest.version(), prevManifest.ssts().size(), prevManifest.totalSizeBytes());
        }
    }

    public Manifest checkpoint() throws Exception {
        long newVersion = prevManifest != null ? prevManifest.version() + 1 : 1L;
        Map<String, Long> offsets = offsetSupplier.currentOffsets();

        long t0 = System.nanoTime();
        Path stagingV = stagingRoot.resolve("v" + newVersion);
        deleteRecursively(stagingV);
        try (Checkpoint cp = Checkpoint.create(db)) {
            cp.createCheckpoint(stagingV.toString());
        }
        long buildNs = System.nanoTime() - t0;

        long t1 = System.nanoTime();
        List<Path> sstFiles = new ArrayList<>();
        List<Path> metaFiles = new ArrayList<>();
        try (Stream<Path> s = Files.list(stagingV)) {
            s.forEach(p -> {
                String n = p.getFileName().toString();
                if (n.equals(LOCK_FILE)) {
                    return;
                }
                if (n.endsWith(".sst")) {
                    sstFiles.add(p);
                } else {
                    metaFiles.add(p);
                }
            });
        }
        Map<String, SstEntry> prevSstByName = new HashMap<>();
        if (prevManifest != null) {
            for (SstEntry e : prevManifest.ssts()) {
                prevSstByName.put(e.name(), e);
            }
        }
        List<Path> newSsts = new ArrayList<>();
        for (Path p : sstFiles) {
            if (!prevSstByName.containsKey(p.getFileName().toString())) {
                newSsts.add(p);
            }
        }
        long diffNs = System.nanoTime() - t1;

        long t2 = System.nanoTime();
        List<CompletableFuture<SstEntry>> sstFutures = new ArrayList<>(newSsts.size());
        for (Path p : newSsts) {
            sstFutures.add(CompletableFuture.supplyAsync(() -> uploadSst(p), uploadExecutor));
        }
        List<CompletableFuture<MetaEntry>> metaFutures = new ArrayList<>(metaFiles.size());
        for (Path p : metaFiles) {
            metaFutures.add(CompletableFuture.supplyAsync(() -> uploadMeta(newVersion, p), uploadExecutor));
        }
        List<SstEntry> newSstEntries = new ArrayList<>(newSsts.size());
        for (CompletableFuture<SstEntry> f : sstFutures) {
            newSstEntries.add(f.join());
        }
        List<MetaEntry> metaEntries = new ArrayList<>(metaFiles.size());
        for (CompletableFuture<MetaEntry> f : metaFutures) {
            metaEntries.add(f.join());
        }
        long uploadNs = System.nanoTime() - t2;

        Map<String, SstEntry> newByName = new HashMap<>();
        for (SstEntry e : newSstEntries) {
            newByName.put(e.name(), e);
        }
        List<SstEntry> allSsts = new ArrayList<>(sstFiles.size());
        for (Path p : sstFiles) {
            String n = p.getFileName().toString();
            SstEntry e = newByName.get(n);
            if (e == null) {
                e = prevSstByName.get(n);
            }
            if (e == null) {
                throw new IllegalStateException("missing sst entry for " + n);
            }
            allSsts.add(e);
        }

        long bytesNew = 0;
        for (SstEntry e : newSstEntries) {
            bytesNew += e.sizeBytes();
        }
        long bytesTotal = 0;
        for (SstEntry e : allSsts) {
            bytesTotal += e.sizeBytes();
        }
        for (MetaEntry e : metaEntries) {
            bytesTotal += e.sizeBytes();
        }

        Manifest manifest = new Manifest(
                newVersion,
                prevManifest != null ? prevManifest.version() : -1L,
                System.currentTimeMillis(),
                offsets,
                allSsts,
                metaEntries,
                bytesTotal
        );
        store.putManifest(manifest);
        prevManifest = manifest;

        deleteRecursively(stagingV);

        if (retainLastN > 0) {
            gc();
        }

        long totalNs = System.nanoTime() - t0;
        metrics.checkpointCreated(manifest, totalNs, buildNs, diffNs, uploadNs,
                newSstEntries.size(), allSsts.size() - newSstEntries.size(), bytesNew);
        return manifest;
    }

    public Manifest latestCommitted() {
        return prevManifest;
    }

    private SstEntry uploadSst(Path p) {
        try {
            String name = p.getFileName().toString();
            long size = Files.size(p);
            acquireUploadBandwidth(size);
            String sha = computeChecksums ? sha256(p) : null;
            store.putSst(name, p);
            return new SstEntry(name, size, sha);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private MetaEntry uploadMeta(long version, Path p) {
        try {
            String name = p.getFileName().toString();
            long size = Files.size(p);
            acquireUploadBandwidth(size);
            store.putMetaFile(version, name, p);
            return new MetaEntry(name, size);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void acquireUploadBandwidth(long bytes) {
        if (uploadBandwidthLimiter == null || bytes <= 0) return;
        try {
            uploadBandwidthLimiter.acquire(bytes);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("interrupted while waiting for upload bandwidth budget", ie);
        }
    }

    private void gc() throws IOException {
        List<Long> versions = store.listManifestVersions();
        if (versions.size() <= retainLastN) {
            return;
        }
        int dropEnd = versions.size() - retainLastN;
        List<Long> drop = versions.subList(0, dropEnd);
        List<Long> retain = versions.subList(dropEnd, versions.size());
        Set<String> liveSsts = new HashSet<>();
        for (long v : retain) {
            store.getManifest(v).ifPresent(m -> {
                for (SstEntry e : m.ssts()) {
                    liveSsts.add(e.name());
                }
            });
        }
        for (long v : drop) {
            store.deleteMetaVersion(v);
            store.deleteManifest(v);
        }
        for (String s : store.listSsts()) {
            if (!liveSsts.contains(s)) {
                store.deleteSst(s);
            }
        }
    }

    @Override
    public void close() {
        if (ownsExecutor) {
            uploadExecutor.shutdown();
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private RocksDB db;
        private Path stagingRoot;
        private CheckpointStore store;
        private OffsetSupplier offsetSupplier;
        private ExecutorService uploadExecutor;
        private int uploadParallelism = 8;
        private boolean computeChecksums = true;
        private int retainLastN = 10;
        private Metrics metrics;
        private BandwidthLimiter uploadBandwidthLimiter;

        public Builder db(RocksDB db) { this.db = db; return this; }
        public Builder stagingRoot(Path p) { this.stagingRoot = p; return this; }
        public Builder store(CheckpointStore s) { this.store = s; return this; }
        public Builder offsetSupplier(OffsetSupplier o) { this.offsetSupplier = o; return this; }
        public Builder uploadExecutor(ExecutorService e) { this.uploadExecutor = e; return this; }
        public Builder uploadParallelism(int n) { this.uploadParallelism = n; return this; }
        public Builder computeChecksums(boolean b) { this.computeChecksums = b; return this; }
        public Builder retainLastN(int n) { this.retainLastN = n; return this; }
        public Builder metrics(Metrics m) { this.metrics = m; return this; }
        public Builder uploadBandwidthLimiter(BandwidthLimiter l) { this.uploadBandwidthLimiter = l; return this; }

        public CheckpointManager build() throws IOException {
            return new CheckpointManager(this);
        }
    }

    private static String sha256(Path p) throws IOException {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            try (InputStream is = Files.newInputStream(p)) {
                byte[] buf = new byte[1 << 20];
                int n;
                while ((n = is.read(buf)) > 0) {
                    md.update(buf, 0, n);
                }
            }
            return HexFormat.of().formatHex(md.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void deleteRecursively(Path p) throws IOException {
        if (!Files.exists(p)) {
            return;
        }
        try (Stream<Path> s = Files.walk(p)) {
            s.sorted(Comparator.reverseOrder()).forEach(x -> {
                try {
                    Files.deleteIfExists(x);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
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
