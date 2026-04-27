package org.example.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public final class SyntheticRecordGenerator {

    private static final char[] ALPHA =
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".toCharArray();

    private final int fieldCount;
    private final int stringLen;
    private final long keyCardinality;
    private final ObjectMapper json = new ObjectMapper();

    public SyntheticRecordGenerator(int fieldCount, int stringLen, long keyCardinality) {
        if (fieldCount <= 0) throw new IllegalArgumentException("fieldCount > 0");
        if (keyCardinality <= 0) throw new IllegalArgumentException("keyCardinality > 0");
        this.fieldCount = fieldCount;
        this.stringLen = stringLen;
        this.keyCardinality = keyCardinality;
    }

    public byte[] keyForIndex(long i) {
        long id = Math.floorMod(i, keyCardinality);
        return ByteBuffer.allocate(Long.BYTES).order(ByteOrder.BIG_ENDIAN).putLong(id).array();
    }

    public byte[] randomValue() {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        Map<String, Object> m = new LinkedHashMap<>(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            String name = "f" + i;
            switch (i & 0x3) {
                case 0 -> m.put(name, randomString(rng, stringLen));
                case 1 -> m.put(name, rng.nextLong());
                case 2 -> m.put(name, rng.nextDouble());
                default -> m.put(name, rng.nextBoolean());
            }
        }
        try {
            return json.writeValueAsBytes(m);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String randomString(ThreadLocalRandom rng, int len) {
        char[] out = new char[len];
        for (int i = 0; i < len; i++) {
            out[i] = ALPHA[rng.nextInt(ALPHA.length)];
        }
        return new String(out);
    }
}
