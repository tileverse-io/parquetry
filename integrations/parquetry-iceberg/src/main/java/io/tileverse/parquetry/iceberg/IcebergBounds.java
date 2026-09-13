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
package io.tileverse.parquetry.iceberg;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

/** Decodes Iceberg manifest geometry bounds: the lower or upper point of a geometry or geography column. */
final class IcebergBounds {

    private static final ValueLayout.OfDouble LE_DOUBLE =
            ValueLayout.JAVA_DOUBLE_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    private static final long PACKED_XY_LENGTH = 16;
    private static final long WKB_POINT_LENGTH = 21;
    private static final long WKB_POINT_X_OFFSET = 5;
    private static final long WKB_POINT_Y_OFFSET = 13;

    private IcebergBounds() {}

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
}
