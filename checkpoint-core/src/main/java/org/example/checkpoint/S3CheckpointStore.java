package org.example.checkpoint;

import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

public final class S3CheckpointStore implements CheckpointStore {

    private final S3AsyncClient s3;
    private final S3TransferManager tm;
    private final boolean ownsClients;
    private final String bucket;
    private final String prefix;
    private final ObjectMapper json = new ObjectMapper();

    public S3CheckpointStore(S3AsyncClient s3, S3TransferManager tm, String bucket, String prefix, boolean ownsClients) {
        this.s3 = s3;
        this.tm = tm;
        this.bucket = bucket;
        this.prefix = normalizePrefix(prefix);
        this.ownsClients = ownsClients;
    }

    public static S3CheckpointStore fromEnv() {
        String region = env("AWS_REGION", "us-east-1");
        String bucket = required("S3_BUCKET");
        String prefix = env("S3_PREFIX", "");
        String endpoint = env("S3_ENDPOINT_URL", null);
        boolean pathStyle = Boolean.parseBoolean(env("S3_PATH_STYLE", "true"));
        long minPartBytes = Long.parseLong(env("S3_MIN_PART_BYTES", String.valueOf(8L * 1024 * 1024)));

        S3AsyncClient.Builder b = S3AsyncClient.builder()
                .region(Region.of(region))
                .multipartEnabled(true)
                .multipartConfiguration(c -> c.minimumPartSizeInBytes(minPartBytes))
                .forcePathStyle(pathStyle);
        if (endpoint != null && !endpoint.isBlank()) {
            b.endpointOverride(URI.create(endpoint));
        }
        S3AsyncClient client = b.build();
        S3TransferManager tm = S3TransferManager.builder().s3Client(client).build();
        return new S3CheckpointStore(client, tm, bucket, prefix, true);
    }

    @Override
    public void putSst(String name, Path local) throws IOException {
        uploadFile(prefix + "ssts/" + name, local);
    }

    @Override
    public void downloadSst(String name, Path target) throws IOException {
        downloadFile(prefix + "ssts/" + name, target);
    }

    @Override
    public void deleteSst(String name) throws IOException {
        deleteOne(prefix + "ssts/" + name);
    }

    @Override
    public Set<String> listSsts() throws IOException {
        String p = prefix + "ssts/";
        return listKeys(p).stream()
                .map(k -> k.substring(p.length()))
                .collect(Collectors.toCollection(HashSet::new));
    }

    @Override
    public void putMetaFile(long version, String name, Path local) throws IOException {
        uploadFile(prefix + "meta/v" + version + "/" + name, local);
    }

    @Override
    public void downloadMetaFile(long version, String name, Path target) throws IOException {
        downloadFile(prefix + "meta/v" + version + "/" + name, target);
    }

    @Override
    public void deleteMetaVersion(long version) throws IOException {
        deleteAllUnder(prefix + "meta/v" + version + "/");
    }

    @Override
    public void putManifest(Manifest m) throws IOException {
        String key = prefix + "manifests/" + formatVersion(m.version()) + ".json";
        byte[] body = json.writerWithDefaultPrettyPrinter().writeValueAsBytes(m);
        PutObjectRequest req = PutObjectRequest.builder()
                .bucket(bucket).key(key).contentType("application/json").build();
        try {
            s3.putObject(req, AsyncRequestBody.fromBytes(body)).join();
        } catch (CompletionException e) {
            throw new IOException("putManifest failed: " + key, unwrap(e));
        }
    }

    @Override
    public Optional<Manifest> getManifest(long version) throws IOException {
        String key = prefix + "manifests/" + formatVersion(version) + ".json";
        GetObjectRequest req = GetObjectRequest.builder().bucket(bucket).key(key).build();
        try {
            byte[] bytes = s3.getObject(req, AsyncResponseTransformer.toBytes()).join().asByteArray();
            return Optional.of(json.readValue(bytes, Manifest.class));
        } catch (CompletionException e) {
            Throwable cause = unwrap(e);
            if (cause instanceof NoSuchKeyException) return Optional.empty();
            throw new IOException("getManifest failed: " + key, cause);
        }
    }

    @Override
    public Optional<Manifest> getLatestManifest() throws IOException {
        List<Long> versions = listManifestVersions();
        if (versions.isEmpty()) return Optional.empty();
        return getManifest(versions.get(versions.size() - 1));
    }

    @Override
    public List<Long> listManifestVersions() throws IOException {
        String p = prefix + "manifests/";
        List<Long> out = new ArrayList<>();
        for (String k : listKeys(p)) {
            String name = k.substring(p.length());
            if (!name.endsWith(".json")) continue;
            try {
                out.add(Long.parseLong(name.substring(0, name.length() - ".json".length())));
            } catch (NumberFormatException ignore) {
                // skip non-conforming names
            }
        }
        out.sort(Long::compareTo);
        return out;
    }

    @Override
    public void deleteManifest(long version) throws IOException {
        deleteOne(prefix + "manifests/" + formatVersion(version) + ".json");
    }

    @Override
    public void close() {
        if (ownsClients) {
            tm.close();
            s3.close();
        }
    }

    private void uploadFile(String key, Path source) throws IOException {
        UploadFileRequest req = UploadFileRequest.builder()
                .putObjectRequest(p -> p.bucket(bucket).key(key))
                .source(source)
                .build();
        try {
            tm.uploadFile(req).completionFuture().join();
        } catch (CompletionException e) {
            throw new IOException("upload failed: " + key, unwrap(e));
        }
    }

    private void downloadFile(String key, Path target) throws IOException {
        Files.createDirectories(target.getParent());
        Path tmp = target.resolveSibling(target.getFileName() + ".part");
        DownloadFileRequest req = DownloadFileRequest.builder()
                .getObjectRequest(g -> g.bucket(bucket).key(key))
                .destination(tmp)
                .build();
        try {
            tm.downloadFile(req).completionFuture().join();
        } catch (CompletionException e) {
            try { Files.deleteIfExists(tmp); } catch (IOException ignored) {}
            throw new IOException("download failed: " + key, unwrap(e));
        }
        Files.move(tmp, target, java.nio.file.StandardCopyOption.ATOMIC_MOVE,
                java.nio.file.StandardCopyOption.REPLACE_EXISTING);
    }

    private void deleteOne(String key) throws IOException {
        try {
            s3.deleteObject(b -> b.bucket(bucket).key(key)).join();
        } catch (CompletionException e) {
            throw new IOException("delete failed: " + key, unwrap(e));
        }
    }

    private void deleteAllUnder(String keyPrefix) throws IOException {
        List<String> keys = listKeys(keyPrefix);
        if (keys.isEmpty()) return;
        for (int i = 0; i < keys.size(); i += 1000) {
            List<ObjectIdentifier> chunk = keys.subList(i, Math.min(i + 1000, keys.size())).stream()
                    .map(k -> ObjectIdentifier.builder().key(k).build())
                    .toList();
            try {
                s3.deleteObjects(b -> b.bucket(bucket).delete(Delete.builder().objects(chunk).build())).join();
            } catch (CompletionException e) {
                throw new IOException("deleteObjects failed under " + keyPrefix, unwrap(e));
            }
        }
    }

    private List<String> listKeys(String keyPrefix) throws IOException {
        List<String> out = new ArrayList<>();
        String token = null;
        try {
            do {
                final String t = token;
                ListObjectsV2Response resp = s3.listObjectsV2(b -> {
                    b.bucket(bucket).prefix(keyPrefix);
                    if (t != null) b.continuationToken(t);
                }).join();
                resp.contents().forEach(o -> out.add(o.key()));
                token = Boolean.TRUE.equals(resp.isTruncated()) ? resp.nextContinuationToken() : null;
            } while (token != null);
        } catch (CompletionException e) {
            throw new IOException("listObjectsV2 failed under " + keyPrefix, unwrap(e));
        }
        return out;
    }

    @SuppressWarnings("unused")
    private boolean exists(String key) {
        HeadObjectRequest req = HeadObjectRequest.builder().bucket(bucket).key(key).build();
        try {
            s3.headObject(req).join();
            return true;
        } catch (CompletionException e) {
            if (unwrap(e) instanceof NoSuchKeyException) return false;
            throw e;
        }
    }

    private static Throwable unwrap(CompletionException e) {
        return e.getCause() != null ? e.getCause() : e;
    }

    private static String formatVersion(long v) {
        return String.format("%020d", v);
    }

    private static String normalizePrefix(String p) {
        if (p == null || p.isBlank()) return "";
        if (p.startsWith("/")) p = p.substring(1);
        return p.endsWith("/") ? p : p + "/";
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        if (v == null) v = System.getProperty(key);
        return v != null && !v.isBlank() ? v : def;
    }

    private static String required(String key) {
        String v = env(key, null);
        if (v == null) throw new IllegalArgumentException(key + " is required");
        return v;
    }
}
