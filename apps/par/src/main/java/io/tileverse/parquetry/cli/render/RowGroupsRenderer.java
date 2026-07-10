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

import java.io.PrintWriter;
import java.util.List;

import io.tileverse.parquetry.data.RowGroupSummary;

/** Renders the per-row-group table for the row-groups command. */
public final class RowGroupsRenderer {

    private RowGroupsRenderer() {}

    /**
     * Writes a fixed-width text table with one row per row group.
     *
     * <p>Columns are left-padded to keep alignment consistent regardless of how large the individual values grow.
     */
    public static void writeText(PrintWriter out, List<RowGroupSummary> rowGroups) {
        out.printf("%-6s  %10s  %18s  %16s%n", "index", "rows", "uncompressed", "compressed");
        out.printf("%-6s  %10s  %18s  %16s%n", "------", "----------", "------------------", "----------------");
        for (RowGroupSummary rg : rowGroups) {
            out.printf(
                    "%-6d  %10d  %18d  %16d%n",
                    rg.index(), rg.rowCount(), rg.totalByteSize(), rg.totalCompressedSize());
        }
        out.flush();
    }

    /**
     * Writes a JSON array where each element represents one row group.
     *
     * <p>Field names match the domain language used across the CLI JSON outputs: "rows" for the row count, byte sizes
     * with explicit "Bytes" suffix.
     */
    public static void writeJson(PrintWriter out, List<RowGroupSummary> rowGroups) {
        Json.writeTo(out, gen -> {
            gen.writeStartArray();
            for (RowGroupSummary rg : rowGroups) {
                gen.writeStartObject();
                gen.writeNumberProperty("index", rg.index());
                gen.writeNumberProperty("rows", rg.rowCount());
                gen.writeNumberProperty("uncompressedBytes", rg.totalByteSize());
                gen.writeNumberProperty("compressedBytes", rg.totalCompressedSize());
                gen.writeEndObject();
            }
            gen.writeEndArray();
        });
    }
}
