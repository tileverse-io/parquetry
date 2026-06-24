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
package io.tileverse.parquetry.arrow.cdi;

import io.tileverse.parquetry.arrow.ipc.ArrowFieldType;
import io.tileverse.parquetry.format.UnsupportedFeatureException;

/**
 * Maps a leaf {@link ArrowFieldType} to its Arrow C Data Interface format string. The composite format strings
 * ({@code +l} list, {@code +s} struct, {@code +m} map) are chosen by the schema exporter from the field kind, since a
 * composite has no leaf type.
 *
 * @see <a href="https://arrow.apache.org/docs/format/CDataInterface.html#data-type-description-format-strings">Arrow
 *     format strings</a>
 */
final class ArrowFormat {

    private ArrowFormat() {
        // utility
    }

    static String of(ArrowFieldType type) {
        return switch (type.kind()) {
            case BOOL -> "b";
            case INT -> integer(type.bitWidth(), type.signed());
            case FLOATING_POINT -> floatingPoint(type.bitWidth());
            case UTF8 -> "u";
            case BINARY -> "z";
            case FIXED_SIZE_BINARY -> "w:" + type.byteWidth();
            case DECIMAL -> "d:" + type.precision() + "," + type.scale();
            case DATE32 -> "tdD";
            case TIME -> time(type);
            case TIMESTAMP -> timestamp(type);
        };
    }

    private static String integer(int bitWidth, boolean signed) {
        return switch (bitWidth) {
            case 8 -> signed ? "c" : "C";
            case 16 -> signed ? "s" : "S";
            case 32 -> signed ? "i" : "I";
            case 64 -> signed ? "l" : "L";
            default -> throw new UnsupportedFeatureException("unsupported integer bit width " + bitWidth);
        };
    }

    private static String floatingPoint(int bitWidth) {
        return switch (bitWidth) {
            case 16 -> "e";
            case 32 -> "f";
            case 64 -> "g";
            default -> throw new UnsupportedFeatureException("unsupported floating point bit width " + bitWidth);
        };
    }

    private static String time(ArrowFieldType type) {
        return "tt" + timeUnitChar(type);
    }

    private static String timestamp(ArrowFieldType type) {
        String timezone = type.utcAdjusted() ? "UTC" : "";
        return "ts" + timeUnitChar(type) + ":" + timezone;
    }

    private static String timeUnitChar(ArrowFieldType type) {
        return switch (type.timeUnit()) {
            case SECOND -> "s";
            case MILLISECOND -> "m";
            case MICROSECOND -> "u";
            case NANOSECOND -> "n";
        };
    }
}
