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
package io.tileverse.parquetry.internal.wkb;

/**
 * The WKB geometry type code: the seven base kinds, the ISO {@code +1000}/{@code +2000}/{@code +3000} Z/M/ZM offsets,
 * the EWKB high-bit flags, and the pure decode functions shared by both parsers.
 *
 * <p>Both encodings are accepted on input. ISO signals Z/M/ZM with the offsets; EWKB signals them with the
 * {@code 0x80000000} (Z) and {@code 0x40000000} (M) flags and adds an optional {@code 0x20000000} SRID flag whose
 * 4-byte payload follows the type code. EWKB sets one or more of the three high bits while ISO codes are small positive
 * ints that never reach them, hence a high-bit test tells the encodings apart unambiguously. The ISO form is what the
 * parquet-format {@code geospatial_types} statistic mandates; {@link #isoType(int)} normalizes either encoding to it.
 */
public final class WkbTypeCode {

    public static final int POINT = 1;
    public static final int LINESTRING = 2;
    public static final int POLYGON = 3;
    public static final int MULTIPOINT = 4;
    public static final int MULTILINESTRING = 5;
    public static final int MULTIPOLYGON = 6;
    public static final int GEOMETRYCOLLECTION = 7;

    public static final int Z_OFFSET = 1000;
    public static final int M_OFFSET = 2000;
    public static final int ZM_OFFSET = 3000;

    public static final int EWKB_FLAG_Z = 0x80000000;
    public static final int EWKB_FLAG_M = 0x40000000;
    public static final int EWKB_FLAG_SRID = 0x20000000;
    private static final int EWKB_FLAG_MASK = EWKB_FLAG_Z | EWKB_FLAG_M | EWKB_FLAG_SRID;

    private WkbTypeCode() {}

    /** Whether {@code rawType} uses the EWKB encoding: any of the three high-bit flags is set. */
    public static boolean isEwkb(int rawType) {
        return (rawType & EWKB_FLAG_MASK) != 0;
    }

    /** Whether an EWKB {@code rawType} announces a 4-byte SRID after the type code. */
    public static boolean hasSrid(int rawType) {
        return (rawType & EWKB_FLAG_SRID) != 0;
    }

    /**
     * The base kind ({@link #POINT} .. {@link #GEOMETRYCOLLECTION}) of {@code rawType}, with the Z/M signal removed.
     */
    public static int baseType(int rawType) {
        if (isEwkb(rawType)) {
            return rawType & ~EWKB_FLAG_MASK;
        }
        if (rawType >= ZM_OFFSET) {
            return rawType - ZM_OFFSET;
        }
        if (rawType >= M_OFFSET) {
            return rawType - M_OFFSET;
        }
        if (rawType >= Z_OFFSET) {
            return rawType - Z_OFFSET;
        }
        return rawType;
    }

    /** The ordinates per coordinate that {@code rawType} declares. */
    public static Dimensions dimensions(int rawType) {
        if (isEwkb(rawType)) {
            boolean hasZ = (rawType & EWKB_FLAG_Z) != 0;
            boolean hasM = (rawType & EWKB_FLAG_M) != 0;
            return Dimensions.of(hasZ, hasM);
        }
        if (rawType >= ZM_OFFSET) {
            return Dimensions.ZM;
        }
        if (rawType >= M_OFFSET) {
            return Dimensions.M;
        }
        if (rawType >= Z_OFFSET) {
            return Dimensions.Z;
        }
        return Dimensions.XY;
    }

    /** The ISO form of {@code rawType}: the base kind plus the Z/M/ZM offset. An ISO input is returned unchanged. */
    public static int isoType(int rawType) {
        if (!isEwkb(rawType)) {
            return rawType;
        }
        int base = baseType(rawType);
        return switch (dimensions(rawType)) {
            case XY -> base;
            case Z -> base + Z_OFFSET;
            case M -> base + M_OFFSET;
            case ZM -> base + ZM_OFFSET;
        };
    }
}
