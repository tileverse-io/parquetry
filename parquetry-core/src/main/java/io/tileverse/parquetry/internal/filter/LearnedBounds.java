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
package io.tileverse.parquetry.internal.filter;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.lang.foreign.MemorySegment;
import java.util.Optional;

import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.Statistics;
import io.tileverse.parquetry.io.Segments;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * The trust rule of a learned page index. It chooses the page bounds kept by a {@link LearnedPages} entry from the
 * statistics of a page header, and it cuts the long ones.
 *
 * <p>A bound is kept only when {@link StatisticsValueDecoder} decodes the bounds of the column type. INT96 columns and
 * columns annotated GEOMETRY or GEOGRAPHY keep no bound. Neither does a column with an annotation ordered unlike its
 * cells, such as a half float. The {@code min_value} and {@code max_value} fields follow the column order and are
 * preferred. The deprecated {@code min} and {@code max} fields predate column orders. Their writers compared binary
 * values as signed bytes, and these fields are kept for the numeric kinds only. A bound of a fixed-width kind is kept
 * as one value. A shorter bound holds no value and is dropped. An empty decimal holds no number and is dropped too.
 *
 * <p>A binary bound compared as unsigned bytes is cut to {@value #MAX_BINARY_BOUND_BYTES} bytes. The prefix of a
 * minimum sorts before the values of the page. A maximum is cut after its last byte below 0xFF in the prefix, and that
 * byte is raised by one. The result sorts after the values of the page. A maximum starting with
 * {@value #MAX_BINARY_BOUND_BYTES} bytes of 0xFF has no such byte and stays whole. A decimal compares as a number, and
 * its bytes stay whole.
 */
final class LearnedBounds {

    /** The bytes kept from a long binary bound. It is the default length of the column index bounds of parquet-java. */
    static final int MAX_BINARY_BOUND_BYTES = 64;

    /** The width given to binary bounds. Their length varies from bound to bound. */
    static final int VARIABLE_WIDTH = 0;

    private static final byte HIGHEST_BYTE = (byte) 0xFF;

    private LearnedBounds() {}

    /**
     * The minimum of a page kept by a learned page index, as a fresh read-only heap copy. It is empty when the trust
     * rule keeps no minimum from {@code statistics}.
     */
    static Optional<MemorySegment> min(Statistics statistics, PrimitiveKind kind, Optional<LogicalType> logicalType) {
        Optional<MemorySegment> trusted = trusted(statistics.minValue(), statistics.min(), kind, logicalType);
        return trusted.map(min -> keptMin(min, kind, logicalType));
    }

    /**
     * The maximum of a page kept by a learned page index, as a fresh read-only heap copy. It is empty when the trust
     * rule keeps no maximum from {@code statistics}.
     */
    static Optional<MemorySegment> max(Statistics statistics, PrimitiveKind kind, Optional<LogicalType> logicalType) {
        Optional<MemorySegment> trusted = trusted(statistics.maxValue(), statistics.max(), kind, logicalType);
        return trusted.map(max -> keptMax(max, kind, logicalType));
    }

    /**
     * The width of a bound of {@code kind}, the bytes of one PLAIN value of a fixed-width kind. It is
     * {@link #VARIABLE_WIDTH} for the binary kinds and for INT96. An INT96 column keeps no bound.
     */
    static int fixedWidth(PrimitiveKind kind) {
        return switch (kind) {
            case BOOLEAN -> Byte.BYTES;
            case INT32, FLOAT -> Integer.BYTES;
            case INT64, DOUBLE -> Long.BYTES;
            case INT96, BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> VARIABLE_WIDTH;
        };
    }

    /** The bound recorded by the writer, when this reader trusts it. */
    private static Optional<MemorySegment> trusted(
            MemorySegment modern, MemorySegment deprecated, PrimitiveKind kind, Optional<LogicalType> logicalType) {
        if (!StatisticsValueDecoder.decodesBoundsOf(kind, logicalType)) {
            return Optional.empty();
        }
        MemorySegment preferred = preferred(modern, deprecated, kind);
        if (!holdsAValue(preferred, kind, logicalType)) {
            return Optional.empty();
        }
        return Optional.of(preferred);
    }

    /**
     * The modern bound when the writer recorded one. Otherwise the deprecated bound of a numeric kind, or
     * {@link MemorySegment#NULL}.
     */
    private static MemorySegment preferred(MemorySegment modern, MemorySegment deprecated, PrimitiveKind kind) {
        if (modern != MemorySegment.NULL) {
            return modern;
        }
        if (comparedAsSignedNumbers(kind)) {
            return deprecated;
        }
        return MemorySegment.NULL;
    }

    /** Whether the pruning tiers compare the cells of {@code kind} in the signed order of the deprecated fields. */
    private static boolean comparedAsSignedNumbers(PrimitiveKind kind) {
        return switch (kind) {
            case BOOLEAN, INT32, INT64, FLOAT, DOUBLE -> true;
            case INT96, BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> false;
        };
    }

    /**
     * Whether {@code bound} holds a value read by the decoder. A bound of a fixed-width kind needs the bytes of one
     * value. An empty decimal holds no number.
     */
    private static boolean holdsAValue(MemorySegment bound, PrimitiveKind kind, Optional<LogicalType> logicalType) {
        if (bound == MemorySegment.NULL || bound.byteSize() < fixedWidth(kind)) {
            return false;
        }
        boolean emptyDecimal = isDecimal(logicalType) && bound.byteSize() == 0;
        return !emptyDecimal;
    }

    private static boolean isDecimal(Optional<LogicalType> logicalType) {
        return logicalType.orElse(null) instanceof LogicalType.Decimal;
    }

    /** A copy of {@code min}, cut to a prefix when it is a long binary bound. */
    private static MemorySegment keptMin(MemorySegment min, PrimitiveKind kind, Optional<LogicalType> logicalType) {
        if (longBinary(min, kind, logicalType)) {
            return Segments.toHeapReadOnly(min.asSlice(0, MAX_BINARY_BOUND_BYTES));
        }
        return copyOf(min, kind);
    }

    /** A copy of {@code max}, cut and raised when it is a long binary bound. */
    private static MemorySegment keptMax(MemorySegment max, PrimitiveKind kind, Optional<LogicalType> logicalType) {
        if (longBinary(max, kind, logicalType)) {
            return raisedPrefixOf(max);
        }
        return copyOf(max, kind);
    }

    /** Whether {@code bound} compares as unsigned bytes and is longer than {@value #MAX_BINARY_BOUND_BYTES} bytes. */
    private static boolean longBinary(MemorySegment bound, PrimitiveKind kind, Optional<LogicalType> logicalType) {
        return comparedAsBytes(kind, logicalType) && bound.byteSize() > MAX_BINARY_BOUND_BYTES;
    }

    /**
     * Whether the bounds of the column compare as unsigned bytes. A prefix of a value then sorts before the value. A
     * decimal compares as a number.
     */
    private static boolean comparedAsBytes(PrimitiveKind kind, Optional<LogicalType> logicalType) {
        boolean binary = kind == PrimitiveKind.BYTE_ARRAY || kind == PrimitiveKind.FIXED_LEN_BYTE_ARRAY;
        return binary && !isDecimal(logicalType);
    }

    /**
     * The prefix of {@code max} up to its last byte below 0xFF among the kept bytes, with that byte raised by one. When
     * the kept bytes are all 0xFF, the result is a whole copy of {@code max}.
     */
    private static MemorySegment raisedPrefixOf(MemorySegment max) {
        int last = lastByteBelowFf(max);
        if (last < 0) {
            return Segments.toHeapReadOnly(max);
        }
        byte[] raised = max.asSlice(0, last + 1L).toArray(JAVA_BYTE);
        raised[last] = (byte) (raised[last] + 1);
        return MemorySegment.ofArray(raised).asReadOnly();
    }

    /** The index of the last byte below 0xFF among the kept bytes of {@code max}, or -1 when there is none. */
    private static int lastByteBelowFf(MemorySegment max) {
        for (int index = MAX_BINARY_BOUND_BYTES - 1; index >= 0; index--) {
            if (max.get(JAVA_BYTE, index) != HIGHEST_BYTE) {
                return index;
            }
        }
        return -1;
    }

    /** A copy of {@code bound}, cut to the width of one value of a fixed-width kind. */
    private static MemorySegment copyOf(MemorySegment bound, PrimitiveKind kind) {
        int width = fixedWidth(kind);
        if (width == VARIABLE_WIDTH) {
            return Segments.toHeapReadOnly(bound);
        }
        return Segments.toHeapReadOnly(bound.asSlice(0, width));
    }
}
