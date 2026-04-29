package org.example.server;

import org.example.checkpoint.ShardAssignment;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public final class Config {

    public final Path restoreDir;
    public final long restoreVersion;          // -1 == latest
    public final int restoreParallelism;
    public final int httpPort;
    public final int httpThreads;

    public final boolean tailKafka;
    public final List<String> kafkaTopics;
    public final String kafkaBootstrap;
    public final String kafkaGroupId;
    public final String kafkaClientId;
    public final int maxPollRecords;

    public final ShardAssignment shard;

    private Config() {
        this.restoreDir = Paths.get(env("RESTORE_DIR", "./data/restored-rocksdb"));
        this.restoreVersion = Long.parseLong(env("RESTORE_VERSION", "-1"));
        this.restoreParallelism = Integer.parseInt(env("RESTORE_PARALLELISM", "8"));
        this.httpPort = Integer.parseInt(env("HTTP_PORT", "8080"));
        this.httpThreads = Integer.parseInt(env("HTTP_THREADS", "16"));

        this.tailKafka = Boolean.parseBoolean(env("TAIL_KAFKA", "false"));
        this.kafkaTopics = List.of(env("KAFKA_TOPICS", "feature-events").split(","));
        this.kafkaBootstrap = env("KAFKA_BOOTSTRAP", "localhost:9092");
        this.kafkaGroupId = env("KAFKA_GROUP_ID", "feature-server");
        this.kafkaClientId = env("KAFKA_CLIENT_ID", "feature-server-1");
        this.maxPollRecords = Integer.parseInt(env("KAFKA_MAX_POLL_RECORDS", "5000"));

        this.shard = ShardAssignment.fromEnv();
    }

    public static Config fromEnv() {
        return new Config();
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        if (v == null) v = System.getProperty(key);
        return v != null && !v.isBlank() ? v : def;
    }
}
