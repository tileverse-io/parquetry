/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.format;

/**
 * Deprecated Parquet "converted type" enum; superseded by {@link LogicalType} but still emitted by older writers, so
 * readers must decode it for backward compatibility.
 *
 * <p>Each constant carries its Thrift wire code in {@link #value()}; resolve incoming i32 codes via
 * {@link #valueOf(int)}.
 */
public enum ConvertedType {
    UTF8(0),
    MAP(1),
    MAP_KEY_VALUE(2),
    LIST(3),
    ENUM(4),
    DECIMAL(5),
    DATE(6),
    TIME_MILLIS(7),
    TIME_MICROS(8),
    TIMESTAMP_MILLIS(9),
    TIMESTAMP_MICROS(10),
    UINT_8(11),
    UINT_16(12),
    UINT_32(13),
    UINT_64(14),
    INT_8(15),
    INT_16(16),
    INT_32(17),
    INT_64(18),
    JSON(19),
    BSON(20),
    INTERVAL(21);

    private final int value;

    ConvertedType(int value) {
        this.value = value;
    }

    /** Thrift wire code for this constant, matching the value defined in {@code parquet.thrift}. */
    public int value() {
        return value;
    }

    /** @throws UnknownCodeException if no defined case carries that code */
    public static ConvertedType valueOf(int code) {
        return switch (code) {
            case 0 -> UTF8;
            case 1 -> MAP;
            case 2 -> MAP_KEY_VALUE;
            case 3 -> LIST;
            case 4 -> ENUM;
            case 5 -> DECIMAL;
            case 6 -> DATE;
            case 7 -> TIME_MILLIS;
            case 8 -> TIME_MICROS;
            case 9 -> TIMESTAMP_MILLIS;
            case 10 -> TIMESTAMP_MICROS;
            case 11 -> UINT_8;
            case 12 -> UINT_16;
            case 13 -> UINT_32;
            case 14 -> UINT_64;
            case 15 -> INT_8;
            case 16 -> INT_16;
            case 17 -> INT_32;
            case 18 -> INT_64;
            case 19 -> JSON;
            case 20 -> BSON;
            case 21 -> INTERVAL;
            default -> throw new UnknownCodeException("Unknown ConvertedType wire code: " + code);
        };
    }
}
