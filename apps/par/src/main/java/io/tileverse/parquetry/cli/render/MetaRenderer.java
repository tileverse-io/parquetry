/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
import java.util.Map;
import java.util.TreeMap;

import io.tileverse.parquetry.data.RowGroupSummary;
import io.tileverse.parquetry.format.FileMetaData;

/** Renders a one-screen file summary from the footer, the public row-group view, and footer key-value metadata. */
public final class MetaRenderer {

    private MetaRenderer() {}

    public static void writeText(
            PrintWriter out,
            FileMetaData footer,
            List<RowGroupSummary> rowGroups,
            int leafColumns,
            Map<String, String> keyValue) {
        long totalBytes =
                rowGroups.stream().mapToLong(RowGroupSummary::totalByteSize).sum();
        long compressed = rowGroups.stream()
                .mapToLong(RowGroupSummary::totalCompressedSize)
                .sum();
        out.printf("created by:     %s%n", footer.createdBy().orElse("(unknown)"));
        out.printf("format version: %d%n", footer.version());
        out.printf("rows:           %d%n", footer.numRows());
        out.printf("row groups:     %d%n", rowGroups.size());
        out.printf("leaf columns:   %d%n", leafColumns);
        out.printf("uncompressed:   %d bytes%n", totalBytes);
        out.printf("compressed:     %d bytes%n", compressed);
        out.printf("key-value:      %s%n", keyNames(keyValue));
        out.flush();
    }

    public static void writeJson(
            PrintWriter out,
            FileMetaData footer,
            List<RowGroupSummary> rowGroups,
            int leafColumns,
            Map<String, String> keyValue) {
        long totalBytes =
                rowGroups.stream().mapToLong(RowGroupSummary::totalByteSize).sum();
        long compressed = rowGroups.stream()
                .mapToLong(RowGroupSummary::totalCompressedSize)
                .sum();
        Json.writeTo(out, gen -> {
            gen.writeStartObject();
            gen.writeStringProperty("createdBy", footer.createdBy().orElse(null));
            gen.writeNumberProperty("formatVersion", footer.version());
            gen.writeNumberProperty("rows", footer.numRows());
            gen.writeNumberProperty("rowGroups", rowGroups.size());
            gen.writeNumberProperty("leafColumns", leafColumns);
            gen.writeNumberProperty("uncompressedBytes", totalBytes);
            gen.writeNumberProperty("compressedBytes", compressed);
            gen.writeName("keyValueMetadata");
            gen.writeStartObject();
            for (Map.Entry<String, String> entry : sorted(keyValue).entrySet()) {
                gen.writeStringProperty(entry.getKey(), entry.getValue());
            }
            gen.writeEndObject();
            gen.writeEndObject();
        });
    }

    /** Lists the metadata keys in sorted order, or a placeholder when the file carries none. */
    private static String keyNames(Map<String, String> keyValue) {
        if (keyValue.isEmpty()) {
            return "(none)";
        }
        return String.join(", ", sorted(keyValue).keySet());
    }

    private static Map<String, String> sorted(Map<String, String> keyValue) {
        return new TreeMap<>(keyValue);
    }
}
