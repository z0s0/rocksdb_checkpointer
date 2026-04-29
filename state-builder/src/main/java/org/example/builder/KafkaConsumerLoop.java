package org.example.builder;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.example.checkpoint.CheckpointManager;
import org.example.checkpoint.Manifest;
import org.example.checkpoint.OffsetSupplier;
import org.example.checkpoint.ShardAssignment;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public final class KafkaConsumerLoop implements Runnable, OffsetSupplier {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerLoop.class);

    private final KafkaConsumer<byte[], byte[]> consumer;
    private final List<String> topics;
    private final ShardAssignment shard;
    private final RocksDbStore rocks;
    private final CheckpointManager checkpointManager;
    private final long checkpointIntervalMs;
    private final long checkpointMaxRecords;

    private final AtomicBoolean stopped = new AtomicBoolean(false);

    public KafkaConsumerLoop(
            KafkaConsumer<byte[], byte[]> consumer,
            List<String> topics,
            ShardAssignment shard,
            RocksDbStore rocks,
            CheckpointManager checkpointManager,
            long checkpointIntervalMs,
            long checkpointMaxRecords
    ) {
        this.consumer = consumer;
        this.topics = topics;
        this.shard = shard;
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
        try {
            List<TopicPartition> owned = computeOwnedPartitions();
            if (owned.isEmpty()) {
                LOG.warn("{} owns no partitions across topics {} — running idle",
                        shard.describe(), topics);
            } else {
                LOG.info("{} owns {} partitions: {}", shard.describe(), owned.size(), owned);
            }

            Manifest latest = checkpointManager.latestCommitted();
            if (latest != null) {
                shard.assertOwns(latest);
            }

            consumer.assign(owned);
            seekOwnedPartitions(owned, latest);

            long lastCheckpointMs = System.currentTimeMillis();
            long recordsSinceCheckpoint = 0;
            long totalRecords = 0;

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

    private void seekOwnedPartitions(List<TopicPartition> owned, Manifest manifest) {
        Map<String, Long> manifestOffsets = manifest != null ? manifest.offsets() : Map.of();
        List<TopicPartition> toSeekBeginning = new ArrayList<>();
        for (TopicPartition tp : owned) {
            String key = tp.topic() + ":" + tp.partition();
            Long off = manifestOffsets.get(key);
            if (off != null) {
                consumer.seek(tp, off);
                LOG.info("seek {} -> {} (manifest v{})", tp, off, manifest.version());
            } else {
                toSeekBeginning.add(tp);
                LOG.info("seek {} -> beginning (no manifest entry)", tp);
            }
        }
        if (!toSeekBeginning.isEmpty()) {
            consumer.seekToBeginning(toSeekBeginning);
        }
    }
}
