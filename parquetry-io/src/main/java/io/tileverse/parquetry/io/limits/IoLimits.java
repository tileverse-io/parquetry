/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.tileverse.parquetry.io.limits;

import java.nio.file.Path;

/**
 * The derived caps the read path honors, computed from a {@link ResourceLimits} by applying conservative fractions.
 * This is the one place those fractions live; the budgets that consume the caps own no sizing of their own.
 */
public record IoLimits(long maxOffHeapBytes, long maxSpillBytes, long maxDecodeBytes, Path spillDir) {

    private static final double OFF_HEAP_FRACTION = 0.1;
    private static final double SPILL_FRACTION = 0.5;
    private static final double DECODE_FRACTION = 0.25;

    public IoLimits {
        if (maxOffHeapBytes <= 0) {
            throw new IllegalArgumentException("maxOffHeapBytes must be > 0, got " + maxOffHeapBytes);
        }
        if (maxSpillBytes <= 0) {
            throw new IllegalArgumentException("maxSpillBytes must be > 0, got " + maxSpillBytes);
        }
        if (maxDecodeBytes <= 0) {
            throw new IllegalArgumentException("maxDecodeBytes must be > 0, got " + maxDecodeBytes);
        }
    }

    /** Derives the caps from {@code limits}, applying the off-heap, spill, and decode fractions. */
    public static IoLimits from(ResourceLimits limits) {
        Path dir = limits.spillDir();
        long offHeap = atLeastOneByte(limits.availableMemoryBytes() * OFF_HEAP_FRACTION);
        long spill = atLeastOneByte(limits.usableDiskBytes(dir) * SPILL_FRACTION);
        long decode = atLeastOneByte(limits.availableMemoryBytes() * DECODE_FRACTION);
        return new IoLimits(offHeap, spill, decode, dir);
    }

    private static long atLeastOneByte(double bytes) {
        return Math.max(1L, (long) bytes);
    }
}
