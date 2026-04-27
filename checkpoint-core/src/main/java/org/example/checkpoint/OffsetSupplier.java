package org.example.checkpoint;

import java.util.Map;

@FunctionalInterface
public interface OffsetSupplier {
    Map<String, Long> currentOffsets();
}
