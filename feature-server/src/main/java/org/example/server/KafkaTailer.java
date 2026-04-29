package org.example.server;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.example.checkpoint.Manifest;
import org.example.checkpoint.ShardAssignment;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public final class KafkaTailer implements Runnable, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaTailer.class);

    private final KafkaConsumer<byte[], byte[]> consumer;
    private final List<String> topics;
    private final ShardAssignment shard;
    private final RocksDB db;
    private final WriteOptions writeOptions;
    private final Manifest seedManifest;
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final Map<String, Long> currentOffsets = new HashMap<>();
    private Thread thread;

    public KafkaTailer(
            KafkaConsumer<byte[], byte[]> consumer,
            List<String> topics,
            ShardAssignment shard,
            RocksDB db,
            Manifest seedManifest
    ) {
        this.consumer = consumer;
        this.topics = topics;
        this.shard = shard;
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
        try {
            List<TopicPartition> owned = computeOwnedPartitions();
            if (owned.isEmpty()) {
                LOG.warn("tailer {} owns no partitions across topics {} — exiting",
                        shard.describe(), topics);
                return;
            }
            LOG.info("tailer {} owns {} partitions: {}", shard.describe(), owned.size(), owned);

            consumer.assign(owned);
            seekOwnedPartitions(owned);

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

    private List<TopicPartition> computeOwnedPartitions() {
        List<TopicPartition> owned = new ArrayList<>();
        for (String topic : topics) {
            List<PartitionInfo> infos = consumer.partitionsFor(topic);
            if (infos == null || infos.isEmpty()) {
                throw new IllegalStateException("topic not found or has no partitions: " + topic);
            }
            int total = infos.size();
            for (int p : shard.ownedPartitions(total)) {
                owned.add(new TopicPartition(topic, p));
            }
        }
        return owned;
    }

    private void seekOwnedPartitions(List<TopicPartition> owned) {
        Map<String, Long> seed = seedManifest != null ? seedManifest.offsets() : Map.of();
        List<TopicPartition> toSeekEnd = new ArrayList<>();
        for (TopicPartition tp : owned) {
            String key = tp.topic() + ":" + tp.partition();
            Long off = seed.get(key);
            if (off != null) {
                consumer.seek(tp, off);
                LOG.info("tailer seek {} -> {} (manifest)", tp, off);
            } else {
                toSeekEnd.add(tp);
                LOG.info("tailer seek {} -> end (no manifest entry)", tp);
            }
        }
        if (!toSeekEnd.isEmpty()) {
            consumer.seekToEnd(toSeekEnd);
        }
    }
}
