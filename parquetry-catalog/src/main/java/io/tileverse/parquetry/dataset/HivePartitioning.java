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
package io.tileverse.parquetry.dataset;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.filter.prune.ColumnStatistics;
import io.tileverse.parquetry.filter.prune.FileStats;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Binds a dataset's Hive partition keys to physical schema columns and turns a file's path partition values into
 * pruning {@link FileStats}. Each bound key contributes an exact ({@code min == max}) per-column statistic, which lets
 * the existing {@link io.tileverse.parquetry.filter.prune.FilePruner} skip a file whose partition value cannot match.
 *
 * <p>A partition key that is not a top-level physical column in the schema (a path-only Hive column) throws: path-only
 * partition columns are not supported yet. This binding is the single point where that support later plugs in.
 *
 * <p>A bound key whose path value cannot be parsed against its column type (an unparseable number, Hive's null-
 * partition sentinel, an unsupported physical kind) contributes no statistic; pruning simply does not apply to that
 * column for that file, and the catalog open still succeeds.
 */
public final class HivePartitioning {

    private final Map<String, BoundColumn> boundColumns;

    private HivePartitioning(Map<String, BoundColumn> boundColumns) {
        this.boundColumns = boundColumns;
    }

    /** A bound partition key's physical type and optional logical-type annotation, both needed to parse path values. */
    private record BoundColumn(PrimitiveKind kind, Optional<LogicalType> logicalType) {}

    /** Whether any partition key was bound (the dataset is partitioned). */
    public boolean isPartitioned() {
        return !boundColumns.isEmpty();
    }

    public static HivePartitioning bind(Iterable<String> partitionKeys, ParquetSchema schema) {
        Objects.requireNonNull(schema, "schema");
        Map<String, BoundColumn> bound = new LinkedHashMap<>();
        for (String key : partitionKeys) {
            bound.put(key, bindKey(key, schema));
        }
        return new HivePartitioning(bound);
    }

    private static BoundColumn bindKey(String key, ParquetSchema schema) {
        SchemaNode node = schema.find(ColumnPath.of(key)).orElseThrow(() -> pathOnly(key));
        if (!(node instanceof SchemaNode.Primitive primitive) || primitive.repetition() == Repetition.REPEATED) {
            throw pathOnly(key);
        }
        return new BoundColumn(primitive.kind(), primitive.logicalType());
    }

    private static IllegalStateException pathOnly(String key) {
        return new IllegalStateException(
                "hive partition column '" + key
                        + "' is not a top-level physical column in the dataset schema; path-only partition columns are not supported yet");
    }

    /**
     * Exact ({@code min == max}) pruning stats for one file from its path {@code partitionValues} and
     * {@code recordCount}. A value that does not parse against its bound column type contributes no stat.
     */
    public FileStats fileStats(Map<String, String> partitionValues, long recordCount) {
        FileStats.Builder builder = FileStats.builder().recordCount(recordCount);
        for (Map.Entry<String, String> entry : partitionValues.entrySet()) {
            addColumnStat(builder, entry.getKey(), entry.getValue());
        }
        return builder.build();
    }

    private void addColumnStat(FileStats.Builder builder, String key, String rawValue) {
        BoundColumn column = boundColumns.get(key);
        if (column == null) {
            return;
        }
        Optional<Value> parsed = PartitionValueParser.parse(rawValue, column.kind(), column.logicalType());
        if (parsed.isEmpty()) {
            return;
        }
        Value value = parsed.get();
        builder.column(
                ColumnPath.of(key), new ColumnStatistics(Optional.of(value), Optional.of(value), OptionalLong.of(0)));
    }
}
