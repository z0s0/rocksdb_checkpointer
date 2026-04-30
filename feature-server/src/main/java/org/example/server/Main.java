package org.example.server;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.example.checkpoint.BandwidthLimiter;
import org.example.checkpoint.CheckpointRestorer;
import org.example.checkpoint.CheckpointStore;
import org.example.checkpoint.CheckpointStores;
import org.example.checkpoint.Manifest;
import org.example.checkpoint.Metrics;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Properties;
import java.util.stream.Stream;

public final class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    static {
        RocksDB.loadLibrary();
    }

    public static void main(String[] args) throws Exception {
        Config cfg = Config.fromEnv();
        LOG.info("starting feature-server shard={} storeType={} restoreDir={} restoreVersion={} httpPort={} tailKafka={}",
                cfg.shard.describe(),
                System.getenv().getOrDefault("CHECKPOINT_STORE_TYPE", "local"),
                cfg.restoreDir, cfg.restoreVersion, cfg.httpPort, cfg.tailKafka);

        CheckpointStore store = CheckpointStores.fromEnv(cfg.shard);
        Metrics metrics = new Metrics();

        // Resolve manifest first and validate against shard before downloading anything.
        Manifest manifest = (cfg.restoreVersion < 0
                ? store.getLatestManifest()
                : store.getManifest(cfg.restoreVersion))
                .orElseThrow(() -> new IllegalStateException(
                        "no manifest available in store for " + cfg.shard.describe()
                                + (cfg.restoreVersion < 0 ? "" : " at version " + cfg.restoreVersion)));
        cfg.shard.assertOwns(manifest);

        deleteRecursively(cfg.restoreDir);
        Files.createDirectories(cfg.restoreDir);

        BandwidthLimiter restoreLimiter = cfg.restoreBandwidthBytesPerSec > 0
                ? new BandwidthLimiter(cfg.restoreBandwidthBytesPerSec)
                : null;
        if (restoreLimiter != null) {
            LOG.info("restore bandwidth limited to {} bytes/sec", cfg.restoreBandwidthBytesPerSec);
        }

        try (CheckpointRestorer restorer = new CheckpointRestorer(store, cfg.restoreParallelism, metrics, restoreLimiter)) {
            restorer.restoreManifest(manifest, cfg.restoreDir);
        }
        // The store stays open in case the tailer needs it; closed on shutdown below.

        Options options = new Options().setCreateIfMissing(false);
        RocksDB db = RocksDB.open(options, cfg.restoreDir.toString());

        KafkaTailer tailer = null;
        if (cfg.tailKafka) {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.kafkaBootstrap);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, cfg.kafkaGroupId);
            props.put(ConsumerConfig.CLIENT_ID_CONFIG, cfg.kafkaClientId);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, cfg.maxPollRecords);
            KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
            tailer = new KafkaTailer(consumer, cfg.kafkaTopics, cfg.shard, db, manifest);
            tailer.start();
        }

        FeatureHttpServer http = new FeatureHttpServer(cfg.httpPort, cfg.httpThreads, db, manifest, tailer);
        http.start();

        KafkaTailer tailerRef = tailer;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("shutdown signal received");
            try { http.close(); } catch (Exception e) { LOG.warn("http close failed", e); }
            if (tailerRef != null) {
                try { tailerRef.close(); } catch (Exception e) { LOG.warn("tailer close failed", e); }
            }
            try { db.close(); } catch (Exception e) { LOG.warn("db close failed", e); }
            options.close();
            try { store.close(); } catch (Exception e) { LOG.warn("store close failed", e); }
        }, "shutdown"));

        Thread.currentThread().join();
    }

    private static void deleteRecursively(Path p) throws IOException {
        if (!Files.exists(p)) return;
        try (Stream<Path> s = Files.walk(p)) {
            s.sorted(Comparator.reverseOrder()).forEach(x -> {
                try { Files.deleteIfExists(x); } catch (IOException e) { throw new UncheckedIOException(e); }
            });
        }
    }
}
