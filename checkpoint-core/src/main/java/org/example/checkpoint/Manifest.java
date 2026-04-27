package org.example.checkpoint;

import java.util.List;
import java.util.Map;

public record Manifest(
        long version,
        long previousVersion,
        long createdAtMs,
        Map<String, Long> offsets,
        List<SstEntry> ssts,
        List<MetaEntry> metaFiles,
        long totalSizeBytes
) {
}
