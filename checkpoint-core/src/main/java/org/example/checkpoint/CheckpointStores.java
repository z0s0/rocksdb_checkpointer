package org.example.checkpoint;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public final class CheckpointStores {

    private CheckpointStores() {
    }

    public static CheckpointStore fromEnv() throws IOException {
        return fromEnv(ShardAssignment.fromEnv());
    }

    public static CheckpointStore fromEnv(ShardAssignment shard) throws IOException {
        String type = env("CHECKPOINT_STORE_TYPE", "local").toLowerCase();
        return switch (type) {
            case "local" -> {
                Path root = Paths.get(env("CHECKPOINT_STORE_DIR", "./data/checkpoints"));
                Path effective = shard != null && shard.isSharded()
                        ? root.resolve(shard.pathSegment())
                        : root;
                yield new LocalFsCheckpointStore(effective);
            }
            case "s3" -> S3CheckpointStore.fromEnv(shard);
            default -> throw new IllegalArgumentException("unknown CHECKPOINT_STORE_TYPE: " + type);
        };
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        if (v == null) v = System.getProperty(key);
        return v != null && !v.isBlank() ? v : def;
    }
}
