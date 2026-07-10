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
package io.tileverse.parquetry.schema;

import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static java.nio.ByteOrder.BIG_ENDIAN;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.UUID;

/**
 * Converts between {@link UUID} and its 16-byte big-endian form, the byte layout Parquet's UUID logical type and the
 * Variant UUID primitive both use (most significant 8 bytes first, then least significant). One definition of the
 * layout, reused by the Variant codec, the FIXED_LEN_BYTE_ARRAY column read path, the write path, and the filter
 * evaluators.
 *
 * <p>{@link #compareSegmentToUuid} orders UUIDs the way Parquet orders FIXED_LEN_BYTE_ARRAY columns: unsigned, byte by
 * byte. That is NOT {@link UUID#compareTo}, which compares the two longs signed and disagrees whenever the high bit of
 * a long differs. Statistics, dictionary, and column-index pruning all rely on unsigned order, hence this is the only
 * correct comparison for a UUID-valued predicate.
 */
public final class UuidConverter {

    /** The fixed width of a UUID column: 16 bytes. */
    public static final int BYTES = 16;

    /** Big-endian 8-byte layout matching Parquet's UUID byte order; unaligned because segments slice at any offset. */
    private static final ValueLayout.OfLong LONG_BE = JAVA_LONG_UNALIGNED.withOrder(BIG_ENDIAN);

    private UuidConverter() {}

    /** Returns the 16 big-endian bytes of {@code uuid}. */
    public static byte[] toBytes(UUID uuid) {
        byte[] out = new byte[BYTES];
        MemorySegment segment = MemorySegment.ofArray(out);
        segment.set(LONG_BE, 0L, uuid.getMostSignificantBits());
        segment.set(LONG_BE, Long.BYTES, uuid.getLeastSignificantBits());
        return out;
    }

    /** Returns the 16 big-endian bytes of {@code uuid} as a read-only on-heap segment. */
    public static MemorySegment toReadOnlySegment(UUID uuid) {
        return MemorySegment.ofArray(toBytes(uuid)).asReadOnly();
    }

    /** Reads the 16-byte big-endian UUID at offset 0 of {@code segment}. */
    public static UUID fromSegment(MemorySegment segment) {
        return fromSegment(segment, 0L);
    }

    /** Reads the 16-byte big-endian UUID at {@code segment[offset .. offset+16)}. */
    public static UUID fromSegment(MemorySegment segment, long offset) {
        long high = segment.get(LONG_BE, offset);
        long low = segment.get(LONG_BE, offset + Long.BYTES);
        return new UUID(high, low);
    }

    /**
     * Unsigned comparison of the 16-byte big-endian UUID at offset 0 of {@code segment} against {@code bound}, agreeing
     * with Parquet's unsigned byte order for FIXED_LEN_BYTE_ARRAY columns. Allocation-free: two long reads. Comparing
     * the high 8 bytes as an unsigned long, then the low 8, is identical to comparing the 16 bytes lexicographically
     * unsigned, because the layout is big-endian.
     */
    public static int compareSegmentToUuid(MemorySegment segment, UUID bound) {
        int high = Long.compareUnsigned(segment.get(LONG_BE, 0L), bound.getMostSignificantBits());
        if (high != 0) {
            return high;
        }
        return Long.compareUnsigned(segment.get(LONG_BE, Long.BYTES), bound.getLeastSignificantBits());
    }
}
