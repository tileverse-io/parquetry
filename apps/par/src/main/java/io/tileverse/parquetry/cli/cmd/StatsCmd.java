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
package io.tileverse.parquetry.cli.cmd;

import java.io.PrintWriter;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaBuilder;
import io.tileverse.parquetry.schema.StandardLists;

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
        Map<ColumnPath, ColumnPath> names = standardNames(footer);
        List<RowGroup> rowGroups = footer.rowGroups();
        for (int rgIndex = 0; rgIndex < rowGroups.size(); rgIndex++) {
            RowGroup rowGroup = rowGroups.get(rgIndex);
            for (ColumnChunk chunk : rowGroup.columns()) {
                Optional<ColumnMetaData> meta = chunk.metaData();
                // Encrypted chunks carry no inline metadata
                if (meta.isEmpty()) {
                    continue;
                }
                ColumnStats entry = buildEntry(rgIndex, meta.get(), names);
                result.add(entry);
            }
        }
        return result;
    }

    /**
     * The name of each column, keyed by its stored path. A list stored in the shape of an older writer is named in the
     * standard form, as the reader and {@code --columns} name it. Both schemas hold the same leaves in the same order.
     */
    private static Map<ColumnPath, ColumnPath> standardNames(FileMetaData footer) {
        ParquetSchema storedSchema = SchemaBuilder.build(footer.schema());
        List<ColumnPath> stored = storedSchema.leafColumns();
        List<ColumnPath> standard = StandardLists.of(storedSchema).leafColumns();
        Map<ColumnPath, ColumnPath> names = HashMap.newHashMap(stored.size());
        for (int leaf = 0; leaf < stored.size(); leaf++) {
            names.put(stored.get(leaf), standard.get(leaf));
        }
        return names;
    }

    private ColumnStats buildEntry(int rgIndex, ColumnMetaData meta, Map<ColumnPath, ColumnPath> names) {
        ColumnPath storedPath = ColumnPath.of(meta.pathInSchema());
        String columnPath = names.getOrDefault(storedPath, storedPath).dot();
        PhysicalType type = meta.type();
        long numValues = meta.numValues();
        OptionalLong nullCount = OptionalLong.empty();
        String minStr = null;
        String maxStr = null;

        Optional<Statistics> statsOpt = meta.statistics();
        if (statsOpt.isPresent()) {
            Statistics stats = statsOpt.get();
            nullCount = stats.nullCount();
            minStr = decodeBound(type, stats.preferredMin());
            maxStr = decodeBound(type, stats.preferredMax());
        }

        return new ColumnStats(rgIndex, columnPath, type.name(), numValues, nullCount, minStr, maxStr);
    }

    /** Renders one PLAIN-encoded statistics bound for display, or {@code null} when the writer recorded none. */
    private String decodeBound(PhysicalType type, MemorySegment bound) {
        if (bound == MemorySegment.NULL) {
            return null;
        }
        return StatsRenderer.decode(type, bound);
    }

    private void renderStats(PrintWriter out, List<ColumnStats> stats) {
        // Null format means no -o flag, which defaults to text
        if (options.format == null || options.format == GlobalOptions.Format.TEXT) {
            StatsRenderer.writeText(out, stats);
            return;
        }
        switch (options.format) {
            case JSON -> StatsRenderer.writeJson(out, stats);
            case JSONL, CSV, TSV, ARROW ->
                throw new ParameterException(
                        spec.commandLine(),
                        "stats does not support --format "
                                + options.format.name().toLowerCase() + "; use text or json");
            // TEXT is handled above; listed here to keep the switch over the enum complete
            case TEXT -> StatsRenderer.writeText(out, stats);
        }
    }
}
