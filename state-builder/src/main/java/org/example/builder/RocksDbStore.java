package org.example.builder;

import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class RocksDbStore implements AutoCloseable {

    static {
        RocksDB.loadLibrary();
    }

    private final Options options;
    private final WriteOptions writeOptions;
    private final RocksDB db;

    public RocksDbStore(Path dataDir, boolean disableWal) throws IOException, RocksDBException {
        Files.createDirectories(dataDir);
        this.options = new Options()
                .setCreateIfMissing(true)
                .setCompressionType(CompressionType.LZ4_COMPRESSION)
                .setBytesPerSync(1L << 20)
                .setWriteBufferSize(64L * 1024 * 1024)
                .setMaxWriteBufferNumber(4)
                .setTargetFileSizeBase(64L * 1024 * 1024)
                .setLevelCompactionDynamicLevelBytes(true);
        this.writeOptions = new WriteOptions().setDisableWAL(disableWal);
        this.db = RocksDB.open(options, dataDir.toString());
    }

    public RocksDB raw() {
        return db;
    }

    public void writeBatch(WriteBatch batch) throws RocksDBException {
        db.write(writeOptions, batch);
    }

    @Override
    public void close() {
        db.close();
        writeOptions.close();
        options.close();
    }
}
