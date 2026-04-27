package org.example.server;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.example.checkpoint.Manifest;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public final class KafkaTailer implements Runnable, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaTailer.class);

    private final KafkaConsumer<byte[], byte[]> consumer;
    private final List<String> topics;
    private final RocksDB db;
    private final WriteOptions writeOptions;
    private final Manifest seedManifest;
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final Map<String, Long> currentOffsets = new HashMap<>();
    private Thread thread;

    public KafkaTailer(KafkaConsumer<byte[], byte[]> consumer, List<String> topics, RocksDB db, Manifest seedManifest) {
        this.consumer = consumer;
        this.topics = topics;
        this.db = db;
        this.writeOptions = new WriteOptions().setDisableWAL(true);
        this.seedManifest = seedManifest;
        if (seedManifest != null) {
            currentOffsets.putAll(seedManifest.offsets());
        }
    }

    public synchronized void start() {
        if (thread != null) return;
        thread = new Thread(this, "kafka-tailer");
        thread.setDaemon(true);
        thread.start();
    }

    public synchronized Map<String, Long> currentOffsets() {
        return Map.copyOf(currentOffsets);
    }

    @Override
    public void run() {
        consumer.subscribe(topics, new ConsumerRebalanceListener() {
            @Override public void onPartitionsRevoked(Collection<TopicPartition> p) {}
            @Override public void onPartitionsAssigned(Collection<TopicPartition> tps) {
                Map<String, Long> seed = seedManifest != null ? seedManifest.offsets() : Map.of();
                for (TopicPartition tp : tps) {
                    Long off = seed.get(tp.topic() + ":" + tp.partition());
                    if (off != null) {
                        consumer.seek(tp, off);
                        LOG.info("tailer seek {}-{} -> {} (manifest)", tp.topic(), tp.partition(), off);
                    } else {
                        consumer.seekToEnd(List.of(tp));
                        LOG.info("tailer seek {}-{} -> end (no manifest entry)", tp.topic(), tp.partition());
                    }
                }
            }
        });

        try {
            while (!stopped.get()) {
                ConsumerRecords<byte[], byte[]> records;
                try {
                    records = consumer.poll(Duration.ofMillis(500));
                } catch (org.apache.kafka.common.errors.WakeupException w) {
                    if (stopped.get()) break;
                    throw w;
                }
                if (records.isEmpty()) continue;

                try (WriteBatch batch = new WriteBatch()) {
                    for (ConsumerRecord<byte[], byte[]> r : records) {
                        if (r.key() == null) continue;
                        if (r.value() == null) {
                            batch.delete(r.key());
                        } else {
                            batch.put(r.key(), r.value());
                        }
                    }
                    db.write(writeOptions, batch);
                } catch (Exception e) {
                    LOG.error("tailer write failed", e);
                    continue;
                }

                synchronized (this) {
                    for (TopicPartition tp : consumer.assignment()) {
                        currentOffsets.put(tp.topic() + ":" + tp.partition(), consumer.position(tp));
                    }
                }
            }
        } finally {
            try { consumer.close(); } catch (Exception ignored) {}
            writeOptions.close();
        }
    }

    @Override
    public void close() {
        stopped.set(true);
        consumer.wakeup();
        if (thread != null) {
            try { thread.join(10_000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
    }
}
