package org.example.checkpoint;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class LocalFsCheckpointStore implements CheckpointStore {

    private final Path root;
    private final Path manifestsDir;
    private final Path sstsDir;
    private final Path metaDir;
    private final ObjectMapper json;

    public LocalFsCheckpointStore(Path root) throws IOException {
        this.root = root;
        this.manifestsDir = root.resolve("manifests");
        this.sstsDir = root.resolve("ssts");
        this.metaDir = root.resolve("meta");
        Files.createDirectories(manifestsDir);
        Files.createDirectories(sstsDir);
        Files.createDirectories(metaDir);
        this.json = new ObjectMapper();
    }

    public Path root() {
        return root;
    }

    @Override
    public void putSst(String name, Path local) throws IOException {
        Path target = sstsDir.resolve(name);
        if (Files.exists(target)) {
            return;
        }
        Path tmp = sstsDir.resolve(name + ".tmp");
        Files.copy(local, tmp, StandardCopyOption.REPLACE_EXISTING);
        Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public void downloadSst(String name, Path target) throws IOException {
        Path source = sstsDir.resolve(name);
        if (!Files.exists(source)) {
            throw new IOException("missing sst in store: " + name);
        }
        Files.createDirectories(target.getParent());
        Path tmp = target.resolveSibling(target.getFileName() + ".part");
        Files.copy(source, tmp, StandardCopyOption.REPLACE_EXISTING);
        Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public void deleteSst(String name) throws IOException {
        Files.deleteIfExists(sstsDir.resolve(name));
    }

    @Override
    public Set<String> listSsts() throws IOException {
        try (Stream<Path> s = Files.list(sstsDir)) {
            return s.map(p -> p.getFileName().toString())
                    .filter(n -> !n.endsWith(".tmp"))
                    .collect(Collectors.toSet());
        }
    }

    @Override
    public void putMetaFile(long version, String name, Path local) throws IOException {
        Path dir = metaDir.resolve("v" + version);
        Files.createDirectories(dir);
        Path target = dir.resolve(name);
        Path tmp = dir.resolve(name + ".tmp");
        Files.copy(local, tmp, StandardCopyOption.REPLACE_EXISTING);
        Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public void downloadMetaFile(long version, String name, Path target) throws IOException {
        Path source = metaDir.resolve("v" + version).resolve(name);
        if (!Files.exists(source)) {
            throw new IOException("missing meta file in store: v" + version + "/" + name);
        }
        Files.createDirectories(target.getParent());
        Path tmp = target.resolveSibling(target.getFileName() + ".part");
        Files.copy(source, tmp, StandardCopyOption.REPLACE_EXISTING);
        Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public void deleteMetaVersion(long version) throws IOException {
        deleteRecursively(metaDir.resolve("v" + version));
    }

    @Override
    public void putManifest(Manifest m) throws IOException {
        Path target = manifestsDir.resolve(formatVersion(m.version()) + ".json");
        Path tmp = manifestsDir.resolve(formatVersion(m.version()) + ".json.tmp");
        json.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(), m);
        Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public Optional<Manifest> getManifest(long version) throws IOException {
        Path p = manifestsDir.resolve(formatVersion(version) + ".json");
        if (!Files.exists(p)) {
            return Optional.empty();
        }
        return Optional.of(json.readValue(p.toFile(), Manifest.class));
    }

    @Override
    public Optional<Manifest> getLatestManifest() throws IOException {
        List<Long> versions = listManifestVersions();
        if (versions.isEmpty()) {
            return Optional.empty();
        }
        return getManifest(versions.get(versions.size() - 1));
    }

    @Override
    public List<Long> listManifestVersions() throws IOException {
        try (Stream<Path> s = Files.list(manifestsDir)) {
            return s.map(p -> p.getFileName().toString())
                    .filter(n -> n.endsWith(".json"))
                    .map(n -> Long.parseLong(n.substring(0, n.length() - ".json".length())))
                    .sorted()
                    .toList();
        }
    }

    @Override
    public void deleteManifest(long version) throws IOException {
        Files.deleteIfExists(manifestsDir.resolve(formatVersion(version) + ".json"));
    }

    private static String formatVersion(long v) {
        return String.format("%020d", v);
    }

    private static void deleteRecursively(Path p) throws IOException {
        if (!Files.exists(p)) {
            return;
        }
        try (Stream<Path> s = Files.walk(p)) {
            s.sorted(Comparator.reverseOrder()).forEach(x -> {
                try {
                    Files.deleteIfExists(x);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }
    }
}
