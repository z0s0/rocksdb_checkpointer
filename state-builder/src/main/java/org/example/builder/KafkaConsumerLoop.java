package org.example.builder;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.example.checkpoint.CheckpointManager;
import org.example.checkpoint.Manifest;
import org.example.checkpoint.OffsetSupplier;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public final class KafkaConsumerLoop implements Runnable, OffsetSupplier {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerLoop.class);

    private final KafkaConsumer<byte[], byte[]> consumer;
    private final List<String> topics;
    private final RocksDbStore rocks;
    private final CheckpointManager checkpointManager;
    private final long checkpointIntervalMs;
    private final long checkpointMaxRecords;

    private final AtomicBoolean stopped = new AtomicBoolean(false);

    public KafkaConsumerLoop(
            KafkaConsumer<byte[], byte[]> consumer,
            List<String> topics,
            RocksDbStore rocks,
            CheckpointManager checkpointManager,
            long checkpointIntervalMs,
            long checkpointMaxRecords
    ) {
        this.consumer = consumer;
        this.topics = topics;
        this.rocks = rocks;
        this.checkpointManager = checkpointManager;
        this.checkpointIntervalMs = checkpointIntervalMs;
        this.checkpointMaxRecords = checkpointMaxRecords;
    }

    public void stop() {
        stopped.set(true);
        consumer.wakeup();
    }

    @Override
    public Map<String, Long> currentOffsets() {
        Map<String, Long> out = new HashMap<>();
        for (TopicPartition tp : consumer.assignment()) {
            out.put(tp.topic() + ":" + tp.partition(), consumer.position(tp));
        }
        return out;
    }

    @Override
    public void run() {
        consumer.subscribe(topics, new SeekToManifestListener());
        long lastCheckpointMs = System.currentTimeMillis();
        long recordsSinceCheckpoint = 0;
        long totalRecords = 0;

        try {
            while (!stopped.get()) {
                ConsumerRecords<byte[], byte[]> records;
                try {
                    records = consumer.poll(Duration.ofSeconds(1));
                } catch (org.apache.kafka.common.errors.WakeupException w) {
                    if (stopped.get()) break;
                    throw w;
                }

                if (!records.isEmpty()) {
                    try (WriteBatch batch = new WriteBatch()) {
                        for (ConsumerRecord<byte[], byte[]> r : records) {
                            byte[] key = r.key();
                            if (key == null) continue;
                            byte[] value = r.value();
                            if (value == null) {
                                batch.delete(key);
                            } else {
                                batch.put(key, value);
                            }
                        }
                        rocks.writeBatch(batch);
                    } catch (Exception e) {
                        throw new RuntimeException("rocksdb write failed", e);
                    }
                    recordsSinceCheckpoint += records.count();
                    totalRecords += records.count();
                }

                long now = System.currentTimeMillis();
                boolean dueByTime = now - lastCheckpointMs >= checkpointIntervalMs;
                boolean dueByCount = recordsSinceCheckpoint >= checkpointMaxRecords;
                if ((dueByTime || dueByCount) && !consumer.assignment().isEmpty()) {
                    LOG.info("triggering checkpoint: recordsSinceLast={} totalRecords={} elapsedMs={}",
                            recordsSinceCheckpoint, totalRecords, now - lastCheckpointMs);
                    try {
                        Manifest m = checkpointManager.checkpoint();
                        LOG.info("checkpoint v{} committed", m.version());
                    } catch (Exception e) {
                        LOG.error("checkpoint failed", e);
                    }
                    lastCheckpointMs = System.currentTimeMillis();
                    recordsSinceCheckpoint = 0;
                }
            }
        } finally {
            try { consumer.close(); } catch (Exception ignored) {}
        }
    }

    private final class SeekToManifestListener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            // No-op: our offsets live in the manifest; nothing to commit to Kafka.
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            Manifest latest = checkpointManager.latestCommitted();
            Map<String, Long> manifestOffsets = latest != null ? latest.offsets() : Map.of();
            for (TopicPartition tp : partitions) {
                Long off = manifestOffsets.get(tp.topic() + ":" + tp.partition());
                if (off != null) {
                    consumer.seek(tp, off);
                    LOG.info("seek {}-{} -> {} (from manifest v{})", tp.topic(), tp.partition(), off, latest.version());
                } else {
                    consumer.seekToBeginning(List.of(tp));
                    LOG.info("seek {}-{} -> beginning (no manifest entry)", tp.topic(), tp.partition());
                }
            }
        }
    }
}
