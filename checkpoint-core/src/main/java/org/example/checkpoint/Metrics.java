package org.example.checkpoint;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

public final class Metrics {

    private static final Logger LOG = LoggerFactory.getLogger("checkpoint.metrics");

    private final ObjectMapper json = new ObjectMapper();

    public void checkpointCreated(
            Manifest m,
            long totalNs,
            long buildNs,
            long diffNs,
            long uploadNs,
            int sstNew,
            int sstReused,
            long bytesNew
    ) {
        Map<String, Object> evt = new LinkedHashMap<>();
        evt.put("event", "checkpoint_created");
        evt.put("version", m.version());
        evt.put("createdAtMs", m.createdAtMs());
        evt.put("totalMs", totalNs / 1_000_000);
        evt.put("buildMs", buildNs / 1_000_000);
        evt.put("diffMs", diffNs / 1_000_000);
        evt.put("uploadMs", uploadNs / 1_000_000);
        evt.put("sstNew", sstNew);
        evt.put("sstReused", sstReused);
        evt.put("bytesNew", bytesNew);
        evt.put("bytesTotal", m.totalSizeBytes());
        evt.put("offsets", m.offsets());
        emit(evt);
    }

    public void checkpointRestored(Manifest m, long totalNs) {
        Map<String, Object> evt = new LinkedHashMap<>();
        evt.put("event", "checkpoint_restored");
        evt.put("version", m.version());
        evt.put("totalMs", totalNs / 1_000_000);
        evt.put("sstCount", m.ssts().size());
        evt.put("metaCount", m.metaFiles().size());
        evt.put("bytesTotal", m.totalSizeBytes());
        evt.put("offsets", m.offsets());
        emit(evt);
    }

    private void emit(Map<String, Object> evt) {
        try {
            LOG.info("{}", json.writeValueAsString(evt));
        } catch (JsonProcessingException e) {
            LOG.warn("metrics serialize failed", e);
        }
    }
}
