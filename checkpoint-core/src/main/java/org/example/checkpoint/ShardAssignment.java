package org.example.checkpoint;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

public record ShardAssignment(int shardId, int shardCount) {

    public static final String DEFAULT_SHARD_ID_VAR = "SHARD_ID";
    public static final String DEFAULT_SHARD_COUNT_VAR = "SHARD_COUNT";
    public static final String DEFAULT_HOSTNAME_VAR = "HOSTNAME";

    public ShardAssignment {
        if (shardCount < 1) {
            throw new IllegalArgumentException("shardCount must be >= 1, got " + shardCount);
        }
        if (shardId < 0 || shardId >= shardCount) {
            throw new IllegalArgumentException("shardId must be in [0, " + shardCount + "), got " + shardId);
        }
    }

    public boolean isSharded() {
        return shardCount > 1;
    }

    public boolean ownsPartition(int partition) {
        return Math.floorMod(partition, shardCount) == shardId;
    }

    public List<Integer> ownedPartitions(int totalPartitions) {
        if (totalPartitions <= 0) return List.of();
        List<Integer> out = new ArrayList<>(totalPartitions / shardCount + 1);
        for (int p = shardId; p < totalPartitions; p += shardCount) {
            out.add(p);
        }
        return out;
    }

    public String pathSegment() {
        return String.format("shard-%05d-of-%05d", shardId, shardCount);
    }

    public void assertOwns(Manifest manifest) {
        for (String key : manifest.offsets().keySet()) {
            int colon = key.lastIndexOf(':');
            if (colon < 0) {
                throw new IllegalStateException("malformed offset key in manifest v"
                        + manifest.version() + ": " + key);
            }
            int partition;
            try {
                partition = Integer.parseInt(key.substring(colon + 1));
            } catch (NumberFormatException e) {
                throw new IllegalStateException("malformed partition in offset key: " + key, e);
            }
            if (!ownsPartition(partition)) {
                throw new IllegalStateException("manifest v" + manifest.version()
                        + " contains offset for " + key + " not owned by " + describe());
            }
        }
    }

    public String describe() {
        return isSharded()
                ? "shard " + shardId + " of " + shardCount
                : "single-shard deployment";
    }

    public static ShardAssignment fromEnv() {
        return fromEnv(DEFAULT_SHARD_ID_VAR, DEFAULT_SHARD_COUNT_VAR, DEFAULT_HOSTNAME_VAR);
    }

    public static ShardAssignment fromEnv(String shardIdVar, String shardCountVar) {
        return fromEnv(shardIdVar, shardCountVar, DEFAULT_HOSTNAME_VAR);
    }

    public static ShardAssignment fromEnv(String shardIdVar, String shardCountVar, String hostnameVar) {
        Integer countOrNull = parseIntOrNull(env(shardCountVar));
        int count = countOrNull == null ? 1 : countOrNull;
        Integer explicitId = parseIntOrNull(env(shardIdVar));
        int id;
        if (explicitId != null) {
            id = explicitId;
        } else if (count == 1) {
            id = 0;
        } else {
            Integer ordinal = parseTrailingOrdinal(hostname(hostnameVar));
            if (ordinal == null) {
                throw new IllegalStateException(shardIdVar + " is not set and "
                        + hostnameVar + " has no trailing ordinal to derive from");
            }
            id = Math.floorMod(ordinal, count);
        }
        return new ShardAssignment(id, count);
    }

    public static Integer parseTrailingOrdinal(String hostname) {
        if (hostname == null || hostname.isEmpty()) return null;
        int i = hostname.length();
        while (i > 0 && Character.isDigit(hostname.charAt(i - 1))) {
            i--;
        }
        if (i == hostname.length()) return null;
        try {
            return Integer.parseInt(hostname.substring(i));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static String hostname(String hostnameVar) {
        String h = env(hostnameVar);
        if (h != null && !h.isBlank()) return h;
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return null;
        }
    }

    private static String env(String key) {
        if (key == null) return null;
        String v = System.getenv(key);
        if (v == null) v = System.getProperty(key);
        return v != null && !v.isBlank() ? v : null;
    }

    private static Integer parseIntOrNull(String s) {
        if (s == null) return null;
        try {
            return Integer.parseInt(s.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
