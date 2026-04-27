package org.example.builder;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.example.checkpoint.CheckpointManager;
import org.example.checkpoint.CheckpointStore;
import org.example.checkpoint.CheckpointStores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public final class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        Config cfg = Config.fromEnv();
        LOG.info("starting state-builder topics={} bootstrap={} dataDir={} storeType={}",
                cfg.kafkaTopics, cfg.kafkaBootstrap, cfg.rocksDbDataDir,
                System.getenv().getOrDefault("CHECKPOINT_STORE_TYPE", "local"));

        RocksDbStore rocks = new RocksDbStore(cfg.rocksDbDataDir, cfg.disableWal);
        CheckpointStore store = CheckpointStores.fromEnv();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.kafkaBootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, cfg.kafkaGroupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, cfg.kafkaClientId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, cfg.maxPollRecords);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, (int) cfg.maxPollIntervalMs);
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);

        // Build the loop first so we can use it as the OffsetSupplier in the manager.
        KafkaConsumerLoop[] loopRef = new KafkaConsumerLoop[1];

        CheckpointManager mgr = CheckpointManager.builder()
                .db(rocks.raw())
                .stagingRoot(cfg.checkpointStagingDir)
                .store(store)
                .offsetSupplier(() -> loopRef[0].currentOffsets())
                .uploadParallelism(cfg.checkpointUploadParallelism)
                .computeChecksums(cfg.checkpointComputeChecksums)
                .retainLastN(cfg.checkpointRetainLastN)
                .build();

        KafkaConsumerLoop loop = new KafkaConsumerLoop(
                consumer, cfg.kafkaTopics, rocks, mgr,
                cfg.checkpointIntervalMs, cfg.checkpointMaxRecords);
        loopRef[0] = loop;

        Thread runner = new Thread(loop, "consumer-loop");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("shutdown signal received");
            loop.stop();
            try {
                runner.join(30_000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            try { mgr.close(); } catch (Exception e) { LOG.warn("manager close failed", e); }
            try { rocks.close(); } catch (Exception e) { LOG.warn("rocks close failed", e); }
            try { store.close(); } catch (Exception e) { LOG.warn("store close failed", e); }
        }, "shutdown"));

        runner.start();
        runner.join();
    }
}
