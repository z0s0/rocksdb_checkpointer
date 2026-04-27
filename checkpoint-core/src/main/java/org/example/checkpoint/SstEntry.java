package org.example.checkpoint;

public record SstEntry(String name, long sizeBytes, String sha256) {
}
