package org.example.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.example.checkpoint.Manifest;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public final class FeatureHttpServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(FeatureHttpServer.class);
    private static final byte[] EMPTY = new byte[0];

    private final HttpServer server;
    private final ExecutorService executor;
    private final RocksDB db;
    private final Manifest manifest;
    private final KafkaTailer tailer;
    private final ObjectMapper json = new ObjectMapper();

    public FeatureHttpServer(int port, int threads, RocksDB db, Manifest manifest, KafkaTailer tailer) throws IOException {
        this.db = db;
        this.manifest = manifest;
        this.tailer = tailer;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.executor = Executors.newFixedThreadPool(threads, namedThreads("http"));
        server.createContext("/features/", this::handleFeatures);
        server.createContext("/health", this::handleHealth);
        server.createContext("/manifest", this::handleManifest);
        server.createContext("/tail", this::handleTail);
        server.setExecutor(executor);
    }

    public void start() {
        server.start();
        LOG.info("HTTP server listening on {}", server.getAddress());
    }

    @Override
    public void close() {
        server.stop(1);
        executor.shutdown();
    }

    private void handleFeatures(HttpExchange ex) throws IOException {
        if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) {
            send(ex, 405, "text/plain", "method not allowed".getBytes(StandardCharsets.UTF_8));
            return;
        }
        String path = ex.getRequestURI().getPath();
        String keyStr = path.substring("/features/".length());
        if (keyStr.isEmpty()) {
            send(ex, 400, "text/plain", "missing key".getBytes(StandardCharsets.UTF_8));
            return;
        }
        long id;
        try {
            id = Long.parseLong(keyStr);
        } catch (NumberFormatException e) {
            send(ex, 400, "text/plain", ("bad key: " + keyStr).getBytes(StandardCharsets.UTF_8));
            return;
        }
        byte[] key = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.BIG_ENDIAN).putLong(id).array();
        byte[] value;
        try {
            value = db.get(key);
        } catch (RocksDBException e) {
            LOG.warn("rocksdb get failed for key={}", id, e);
            send(ex, 500, "text/plain", e.getMessage().getBytes(StandardCharsets.UTF_8));
            return;
        }
        if (value == null) {
            send(ex, 404, "application/json", "{\"error\":\"not_found\"}".getBytes(StandardCharsets.UTF_8));
            return;
        }
        send(ex, 200, "application/json", value);
    }

    private void handleHealth(HttpExchange ex) throws IOException {
        send(ex, 200, "application/json", "{\"status\":\"ok\"}".getBytes(StandardCharsets.UTF_8));
    }

    private void handleManifest(HttpExchange ex) throws IOException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("version", manifest.version());
        body.put("previousVersion", manifest.previousVersion());
        body.put("createdAtMs", manifest.createdAtMs());
        body.put("sstCount", manifest.ssts().size());
        body.put("metaCount", manifest.metaFiles().size());
        body.put("totalSizeBytes", manifest.totalSizeBytes());
        body.put("offsets", manifest.offsets());
        send(ex, 200, "application/json", json.writeValueAsBytes(body));
    }

    private void handleTail(HttpExchange ex) throws IOException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("enabled", tailer != null);
        if (tailer != null) {
            body.put("currentOffsets", tailer.currentOffsets());
        }
        body.put("manifestOffsets", manifest.offsets());
        send(ex, 200, "application/json", json.writeValueAsBytes(body));
    }

    private static void send(HttpExchange ex, int code, String contentType, byte[] body) throws IOException {
        ex.getResponseHeaders().add("Content-Type", contentType);
        if (body.length == 0) {
            ex.sendResponseHeaders(code, -1);
        } else {
            ex.sendResponseHeaders(code, body.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(body);
            }
        }
    }

    private static ThreadFactory namedThreads(String prefix) {
        AtomicInteger n = new AtomicInteger();
        return r -> {
            Thread t = new Thread(r, prefix + "-" + n.getAndIncrement());
            t.setDaemon(true);
            return t;
        };
    }
}
