package org.example.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public final class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        String bootstrap = env("KAFKA_BOOTSTRAP", "localhost:9092");
        String topic = env("KAFKA_TOPIC", "feature-events");
        long totalRecords = Long.parseLong(env("TOTAL_RECORDS", "1000000"));
        long keyCardinality = Long.parseLong(env("KEY_CARDINALITY", "100000"));
        long targetRatePerSec = Long.parseLong(env("TARGET_RATE_PER_SEC", "0")); // 0 = no throttle
        int fieldCount = Integer.parseInt(env("VALUE_FIELD_COUNT", "100"));
        int stringLen = Integer.parseInt(env("VALUE_STRING_LEN", "20"));

        LOG.info("bench-producer bootstrap={} topic={} totalRecords={} keyCardinality={} rate={}/s fields={}",
                bootstrap, topic, totalRecords, keyCardinality, targetRatePerSec, fieldCount);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "20");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 64 * 1024);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 64L * 1024 * 1024);

        SyntheticRecordGenerator gen = new SyntheticRecordGenerator(fieldCount, stringLen, keyCardinality);
        AtomicLong sent = new AtomicLong();
        AtomicLong acked = new AtomicLong();
        AtomicLong failed = new AtomicLong();

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
            long start = System.nanoTime();
            long batchSize = 1000;
            long batchStart = System.nanoTime();
            long lastLog = System.nanoTime();

            for (long i = 0; i < totalRecords; i++) {
                byte[] key = gen.keyForIndex(i);
                byte[] value = gen.randomValue();
                ProducerRecord<byte[], byte[]> rec = new ProducerRecord<>(topic, key, value);
                producer.send(rec, (md, ex) -> {
                    if (ex == null) {
                        acked.incrementAndGet();
                    } else {
                        failed.incrementAndGet();
                        LOG.warn("send failed: {}", ex.toString());
                    }
                });
                sent.incrementAndGet();

                if ((i + 1) % batchSize == 0) {
                    if (targetRatePerSec > 0) {
                        long elapsedNs = System.nanoTime() - batchStart;
                        long expectedNs = (batchSize * 1_000_000_000L) / targetRatePerSec;
                        if (elapsedNs < expectedNs) {
                            long sleepMs = (expectedNs - elapsedNs) / 1_000_000;
                            if (sleepMs > 0) Thread.sleep(sleepMs);
                        }
                        batchStart = System.nanoTime();
                    }
                    long now = System.nanoTime();
                    if (now - lastLog > 5_000_000_000L) {
                        long s = sent.get();
                        long a = acked.get();
                        double elapsedSec = (now - start) / 1e9;
                        LOG.info("sent={} acked={} failed={} elapsedSec={} rate={}/s",
                                s, a, failed.get(),
                                String.format("%.1f", elapsedSec),
                                (long) (s / Math.max(elapsedSec, 1e-9)));
                        lastLog = now;
                    }
                }
            }
            producer.flush();
            double elapsedSec = (System.nanoTime() - start) / 1e9;
            LOG.info("done sent={} acked={} failed={} elapsedSec={} rate={}/s",
                    sent.get(), acked.get(), failed.get(),
                    String.format("%.1f", elapsedSec),
                    (long) (sent.get() / Math.max(elapsedSec, 1e-9)));
        }
    }

    private static String env(String k, String def) {
        String v = System.getenv(k);
        if (v == null) v = System.getProperty(k);
        return v != null && !v.isBlank() ? v : def;
    }
}
