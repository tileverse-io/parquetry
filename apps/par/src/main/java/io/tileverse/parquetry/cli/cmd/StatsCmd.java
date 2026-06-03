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
package io.tileverse.parquetry.cli.cmd;

import java.io.PrintWriter;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.Callable;

import io.tileverse.parquetry.cli.GlobalOptions;
import io.tileverse.parquetry.cli.StorageOptions;
import io.tileverse.parquetry.cli.UriResolver;
import io.tileverse.parquetry.cli.render.StatsRenderer;
import io.tileverse.parquetry.cli.render.StatsRenderer.ColumnStats;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.format.Statistics;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

@Command(name = "stats", description = "Print per-column statistics.")
public final class StatsCmd implements Callable<Integer> {

    @Parameters(index = "0", paramLabel = "<uri>", description = "Parquet file path or URI.")
    private String uri;

    @Mixin
    private GlobalOptions options;

    @ArgGroup(validate = false, heading = StorageOptions.HEADING)
    private StorageOptions storage = new StorageOptions();

    @Spec
    private CommandSpec spec;

    @Override
    public Integer call() throws Exception {
        try (UriResolver.OpenFile open = UriResolver.open(uri, storage.toProperties())) {
            FileMetaData footer = ParquetFormat.readFooter(open.source());
            List<ColumnStats> stats = collectStats(footer);
            renderStats(spec.commandLine().getOut(), stats);
            return 0;
        }
    }

    private List<ColumnStats> collectStats(FileMetaData footer) {
        List<ColumnStats> result = new ArrayList<>();
        List<RowGroup> rowGroups = footer.rowGroups();
        for (int rgIndex = 0; rgIndex < rowGroups.size(); rgIndex++) {
            RowGroup rowGroup = rowGroups.get(rgIndex);
            for (ColumnChunk chunk : rowGroup.columns()) {
                Optional<ColumnMetaData> meta = chunk.metaData();
                // Encrypted chunks carry no inline metadata
                if (meta.isEmpty()) {
                    continue;
                }
                ColumnStats entry = buildEntry(rgIndex, meta.get());
                result.add(entry);
            }
        }
        return result;
    }

    private ColumnStats buildEntry(int rgIndex, ColumnMetaData meta) {
        String columnPath = String.join(".", meta.pathInSchema());
        PhysicalType type = meta.type();
        long numValues = meta.numValues();
        OptionalLong nullCount = OptionalLong.empty();
        String minStr = null;
        String maxStr = null;

        Optional<Statistics> statsOpt = meta.statistics();
        if (statsOpt.isPresent()) {
            Statistics stats = statsOpt.get();
            nullCount = stats.nullCount();
            minStr = decodeMinMax(type, stats.minValue(), stats.min());
            maxStr = decodeMinMax(type, stats.maxValue(), stats.max());
        }

        return new ColumnStats(rgIndex, columnPath, type.name(), numValues, nullCount, minStr, maxStr);
    }

    /**
     * Picks the modern minValue/maxValue payload when present, falling back to the legacy min/max field. Newer parquet
     * writers populate the modern fields; older writers use the legacy pair.
     */
    private String decodeMinMax(PhysicalType type, MemorySegment modern, MemorySegment legacy) {
        MemorySegment chosen = (modern != MemorySegment.NULL) ? modern : legacy;
        if (chosen == MemorySegment.NULL) {
            return null;
        }
        return StatsRenderer.decode(type, chosen);
    }

    private void renderStats(PrintWriter out, List<ColumnStats> stats) {
        // Null format means no -o flag, which defaults to text
        if (options.format == null || options.format == GlobalOptions.Format.TEXT) {
            StatsRenderer.writeText(out, stats);
            return;
        }
        switch (options.format) {
            case JSON -> StatsRenderer.writeJson(out, stats);
            case JSONL, CSV, TSV ->
                throw new ParameterException(
                        spec.commandLine(),
                        "stats does not support --format "
                                + options.format.name().toLowerCase() + "; use text or json");
            // TEXT is handled above; listed here to keep the switch over the enum complete
            case TEXT -> StatsRenderer.writeText(out, stats);
        }
    }
}
