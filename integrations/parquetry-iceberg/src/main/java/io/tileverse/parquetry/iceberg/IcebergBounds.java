/*
 * Copyright (c) 2026 Multivers.io
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
package io.tileverse.parquetry.iceberg;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.UUID;

import io.tileverse.parquetry.filter.Value;

/** Decodes Iceberg manifest column bounds: typed single values and geometry/geography points. */
final class IcebergBounds {

    private static final ValueLayout.OfInt LE_INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfLong LE_LONG =
            ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfDouble LE_DOUBLE =
            ValueLayout.JAVA_DOUBLE_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfLong BE_LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);

    private static final long PACKED_XY_LENGTH = 16;
    private static final long WKB_POINT_LENGTH = 21;
    private static final long WKB_POINT_X_OFFSET = 5;
    private static final long WKB_POINT_Y_OFFSET = 13;

    private IcebergBounds() {}

    /** Decodes a single-value bound for an Iceberg primitive type into a filter {@link Value}. */
    public static Value decodeValue(String icebergType, MemorySegment bytes) {
        return switch (icebergType) {
            case "int" -> new Value.IntVal(readLeInt(bytes));
            case "long" -> new Value.LongVal(readLeLong(bytes));
            case "float" -> new Value.FloatVal(Float.intBitsToFloat(readLeInt(bytes)));
            case "double" -> new Value.DoubleVal(Double.longBitsToDouble(readLeLong(bytes)));
            case "boolean" -> new Value.BoolVal(readBoolean(bytes));
            case "date" -> new Value.DateVal(LocalDate.ofEpochDay(readLeInt(bytes)));
            case "string" -> new Value.StringVal(utf8(bytes));
            case "uuid" -> new Value.UuidVal(readUuid(bytes));
            default -> throw new IcebergFormatException("unsupported bound type: " + icebergType);
        };
    }

    /** Decodes a geometry/geography bound point into {x, y}; 16-byte packed_xy or 21-byte wkb_point. */
    public static double[] decodePoint(MemorySegment bytes) {
        long length = bytes.byteSize();
        if (length == PACKED_XY_LENGTH) {
            return new double[] {bytes.get(LE_DOUBLE, 0), bytes.get(LE_DOUBLE, 8)};
        }
        if (length == WKB_POINT_LENGTH) {
            return new double[] {bytes.get(LE_DOUBLE, WKB_POINT_X_OFFSET), bytes.get(LE_DOUBLE, WKB_POINT_Y_OFFSET)};
        }
        throw new IcebergFormatException("unexpected geometry bound length: " + length);
    }

    private static int readLeInt(MemorySegment bytes) {
        requireLength(bytes, 4, "4-byte");
        return bytes.get(LE_INT, 0);
    }

    private static long readLeLong(MemorySegment bytes) {
        requireLength(bytes, 8, "8-byte");
        return bytes.get(LE_LONG, 0);
    }

    private static boolean readBoolean(MemorySegment bytes) {
        requireLength(bytes, 1, "1-byte");
        return bytes.get(JAVA_BYTE, 0) != 0;
    }

    private static UUID readUuid(MemorySegment bytes) {
        requireLength(bytes, 16, "16-byte");
        long high = bytes.get(BE_LONG, 0);
        long low = bytes.get(BE_LONG, 8);
        return new UUID(high, low);
    }

    private static String utf8(MemorySegment bytes) {
        byte[] raw = bytes.toArray(JAVA_BYTE);
        return new String(raw, StandardCharsets.UTF_8);
    }

    private static void requireLength(MemorySegment bytes, long expected, String description) {
        if (bytes.byteSize() < expected) {
            throw new IcebergFormatException("bound too short: expected at least a " + description + " value but got "
                    + bytes.byteSize() + " bytes");
        }
    }
}
