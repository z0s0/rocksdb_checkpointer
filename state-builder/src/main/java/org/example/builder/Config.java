package org.example.builder;

import org.example.checkpoint.ShardAssignment;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public final class Config {

    public final List<String> kafkaTopics;
    public final String kafkaBootstrap;
    public final String kafkaGroupId;
    public final String kafkaClientId;
    public final int maxPollRecords;
    public final long maxPollIntervalMs;

    public final Path rocksDbDataDir;
    public final boolean disableWal;

    public final Path checkpointStagingDir;
    public final long checkpointIntervalMs;
    public final long checkpointMaxRecords;
    public final int checkpointRetainLastN;
    public final int checkpointUploadParallelism;
    public final boolean checkpointComputeChecksums;

    public final ShardAssignment shard;

    private Config() {
        this.kafkaTopics = List.of(env("KAFKA_TOPICS", "feature-events").split(","));
        this.kafkaBootstrap = env("KAFKA_BOOTSTRAP", "localhost:9092");
        this.kafkaGroupId = env("KAFKA_GROUP_ID", "state-builder");
        this.kafkaClientId = env("KAFKA_CLIENT_ID", "state-builder-1");
        this.maxPollRecords = Integer.parseInt(env("KAFKA_MAX_POLL_RECORDS", "5000"));
        this.maxPollIntervalMs = Long.parseLong(env("KAFKA_MAX_POLL_INTERVAL_MS", "1200000"));

        this.rocksDbDataDir = Paths.get(env("ROCKSDB_DATA_DIR", "./data/rocksdb"));
        this.disableWal = Boolean.parseBoolean(env("ROCKSDB_DISABLE_WAL", "true"));

        this.checkpointStagingDir = Paths.get(env("CHECKPOINT_STAGING_DIR", "./data/staging"));
        this.checkpointIntervalMs = Long.parseLong(env("CHECKPOINT_INTERVAL_MS", "60000"));
        this.checkpointMaxRecords = Long.parseLong(env("CHECKPOINT_MAX_RECORDS", "1000000"));
        this.checkpointRetainLastN = Integer.parseInt(env("CHECKPOINT_RETAIN_LAST_N", "10"));
        this.checkpointUploadParallelism = Integer.parseInt(env("CHECKPOINT_UPLOAD_PARALLELISM", "8"));
        this.checkpointComputeChecksums = Boolean.parseBoolean(env("CHECKPOINT_COMPUTE_CHECKSUMS", "true"));

        this.shard = ShardAssignment.fromEnv();
    }

    public static Config fromEnv() {
        return new Config();
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        if (v == null) {
            v = System.getProperty(key);
        }
        return v != null && !v.isBlank() ? v : def;
    }
}
