package org.example.checkpoint;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Token-bucket rate limiter on bytes. Acquire blocks until enough tokens are available;
 * tokens refill at refillBytesPerSec up to capacityBytes.
 *
 * <p>Soft cap: paces the start of operations. Concurrent downloads/uploads can briefly
 * spike instantaneous bandwidth above target, but average rate over multiple files
 * tracks the configured limit.
 */
public final class BandwidthLimiter {

    private final long capacityBytes;
    private final long refillBytesPerSec;
    private final Object lock = new Object();
    private double availableBytes;
    private long lastRefillNanos;
    private final AtomicLong totalWaitNanos = new AtomicLong();

    public BandwidthLimiter(long bytesPerSec) {
        this(bytesPerSec, Math.max(bytesPerSec * 2L, 1L << 20));
    }

    public BandwidthLimiter(long refillBytesPerSec, long capacityBytes) {
        if (refillBytesPerSec <= 0) {
            throw new IllegalArgumentException("refillBytesPerSec must be > 0");
        }
        if (capacityBytes <= 0) {
            throw new IllegalArgumentException("capacityBytes must be > 0");
        }
        this.refillBytesPerSec = refillBytesPerSec;
        this.capacityBytes = capacityBytes;
        this.availableBytes = capacityBytes;
        this.lastRefillNanos = System.nanoTime();
    }

    public long refillBytesPerSec() {
        return refillBytesPerSec;
    }

    public long totalWaitNanos() {
        return totalWaitNanos.get();
    }

    public void acquire(long bytes) throws InterruptedException {
        if (bytes <= 0) return;
        long startNanos = System.nanoTime();
        while (true) {
            long sleepMs;
            synchronized (lock) {
                refillLocked();
                if (availableBytes >= bytes) {
                    availableBytes -= bytes;
                    long waited = System.nanoTime() - startNanos;
                    if (waited > 0) totalWaitNanos.addAndGet(waited);
                    return;
                }
                double needed = bytes - availableBytes;
                long sleepNanos = (long) ((needed * 1_000_000_000d) / refillBytesPerSec);
                sleepMs = Math.max(1L, sleepNanos / 1_000_000L);
            }
            Thread.sleep(sleepMs);
        }
    }

    private void refillLocked() {
        long now = System.nanoTime();
        long elapsedNanos = now - lastRefillNanos;
        if (elapsedNanos <= 0) return;
        double add = (elapsedNanos * (double) refillBytesPerSec) / 1_000_000_000d;
        if (add > 0) {
            availableBytes = Math.min(capacityBytes, availableBytes + add);
            lastRefillNanos = now;
        }
    }

    /**
     * Parses a rate string with optional suffix (case-insensitive):
     * {@code "1500000"}, {@code "1.5M"}, {@code "10G"}, {@code "500k"}.
     * Suffixes are binary (1K = 1024, 1M = 2^20, 1G = 2^30).
     * Returns 0 for null, blank, or "0".
     */
    public static long parseRate(String s) {
        if (s == null) return 0;
        String t = s.trim().toLowerCase();
        if (t.isEmpty() || t.equals("0")) return 0;
        long mult = 1L;
        if (t.endsWith("gb")) { mult = 1L << 30; t = t.substring(0, t.length() - 2); }
        else if (t.endsWith("g")) { mult = 1L << 30; t = t.substring(0, t.length() - 1); }
        else if (t.endsWith("mb")) { mult = 1L << 20; t = t.substring(0, t.length() - 2); }
        else if (t.endsWith("m")) { mult = 1L << 20; t = t.substring(0, t.length() - 1); }
        else if (t.endsWith("kb")) { mult = 1L << 10; t = t.substring(0, t.length() - 2); }
        else if (t.endsWith("k")) { mult = 1L << 10; t = t.substring(0, t.length() - 1); }
        double v = Double.parseDouble(t.trim());
        return (long) (v * mult);
    }
}
