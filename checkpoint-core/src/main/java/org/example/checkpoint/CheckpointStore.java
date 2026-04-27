package org.example.checkpoint;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface CheckpointStore extends AutoCloseable {

    @Override
    default void close() {
    }


    void putSst(String name, Path localFile) throws IOException;

    void downloadSst(String name, Path target) throws IOException;

    void deleteSst(String name) throws IOException;

    Set<String> listSsts() throws IOException;

    void putMetaFile(long version, String name, Path localFile) throws IOException;

    void downloadMetaFile(long version, String name, Path target) throws IOException;

    void deleteMetaVersion(long version) throws IOException;

    void putManifest(Manifest manifest) throws IOException;

    Optional<Manifest> getManifest(long version) throws IOException;

    Optional<Manifest> getLatestManifest() throws IOException;

    List<Long> listManifestVersions() throws IOException;

    void deleteManifest(long version) throws IOException;
}
