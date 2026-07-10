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
package io.tileverse.parquetry.cli.render;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.io.PrintWriter;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.OptionalLong;

import io.tileverse.parquetry.format.ParquetLayouts;
import io.tileverse.parquetry.format.PhysicalType;

/** Renders per-column statistics for the stats command. */
public final class StatsRenderer {

    private StatsRenderer() {}

    /**
     * One row of statistics output: the row-group index, column path, physical type, value count, null count, and
     * decoded min/max. Min and max are null when the writer did not record them or when the type cannot be decoded
     * (e.g., INT96).
     */
    public record ColumnStats(
            int rowGroup, String column, String type, long numValues, OptionalLong nullCount, String min, String max) {}

    /**
     * Writes a fixed-width text table with one row per (row-group, column) pair.
     *
     * <p>Absent values (null count, min, max) are rendered as "-" to keep alignment unambiguous.
     */
    public static void writeText(PrintWriter out, List<ColumnStats> rows) {
        out.printf(
                "%-8s  %-20s  %-22s  %10s  %10s  %16s  %16s%n",
                "rowGroup", "column", "type", "values", "nulls", "min", "max");
        out.printf(
                "%-8s  %-20s  %-22s  %10s  %10s  %16s  %16s%n",
                "--------",
                "--------------------",
                "----------------------",
                "----------",
                "----------",
                "----------------",
                "----------------");
        for (ColumnStats row : rows) {
            String nulls =
                    row.nullCount().isPresent() ? Long.toString(row.nullCount().getAsLong()) : "-";
            String minVal = row.min() != null ? row.min() : "-";
            String maxVal = row.max() != null ? row.max() : "-";
            out.printf(
                    "%-8d  %-20s  %-22s  %10d  %10s  %16s  %16s%n",
                    row.rowGroup(), row.column(), row.type(), row.numValues(), nulls, minVal, maxVal);
        }
        out.flush();
    }

    /**
     * Writes a JSON array where each element represents one (row-group, column) statistics entry.
     *
     * <p>Absent numeric null counts and absent string min/max are serialized as JSON null to keep the schema uniform
     * across rows.
     */
    public static void writeJson(PrintWriter out, List<ColumnStats> rows) {
        Json.writeTo(out, gen -> {
            gen.writeStartArray();
            for (ColumnStats row : rows) {
                gen.writeStartObject();
                gen.writeNumberProperty("rowGroup", row.rowGroup());
                gen.writeStringProperty("column", row.column());
                gen.writeStringProperty("type", row.type());
                gen.writeNumberProperty("numValues", row.numValues());
                if (row.nullCount().isPresent()) {
                    gen.writeNumberProperty("nullCount", row.nullCount().getAsLong());
                } else {
                    gen.writeStringProperty("nullCount", null);
                }
                gen.writeStringProperty("min", row.min());
                gen.writeStringProperty("max", row.max());
                gen.writeEndObject();
            }
            gen.writeEndArray();
        });
    }

    /**
     * Decodes a PLAIN-encoded min or max byte payload into a human-readable String.
     *
     * <p>Parquet PLAIN encoding is little-endian for numeric types. The method returns null when the type is
     * unsupported (INT96) or when the payload is too short to hold a complete value.
     */
    public static String decode(PhysicalType type, MemorySegment raw) {
        long size = raw.byteSize();
        return switch (type) {
            case BOOLEAN -> size >= 1 ? Boolean.toString(raw.get(JAVA_BYTE, 0) != 0) : null;
            case INT32 -> size >= 4 ? Integer.toString(raw.get(ParquetLayouts.INT32, 0)) : null;
            case INT64 -> size >= 8 ? Long.toString(raw.get(ParquetLayouts.INT64, 0)) : null;
            case FLOAT -> size >= 4 ? Float.toString(raw.get(ParquetLayouts.FLOAT, 0)) : null;
            case DOUBLE -> size >= 8 ? Double.toString(raw.get(ParquetLayouts.DOUBLE, 0)) : null;
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> new String(raw.toArray(JAVA_BYTE), StandardCharsets.UTF_8);
            // INT96 is a legacy 12-byte nanosecond timestamp with no standard decoding at this tier
            case INT96 -> null;
        };
    }
}
