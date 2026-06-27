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

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import io.tileverse.parquetry.columnar.ConstantLeaves;
import io.tileverse.parquetry.filter.ConstantColumn;
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
 * Binds a dataset's Hive partition keys and turns a file's path partition values into pruning {@link FileStats}. Each
 * bound key contributes an exact ({@code min == max}) per-column statistic, which lets the existing
 * {@link io.tileverse.parquetry.filter.prune.FilePruner} skip a file whose partition value cannot match.
 *
 * <p>A key that is a top-level physical column in the schema binds PHYSICAL: its path value is parsed against the
 * column's physical and logical type. A key with no matching schema column (a path-only Hive column) binds SYNTHETIC:
 * its type is inferred from the path values across all files ({@link PartitionTypeInference#inferConsistent}), and the
 * dataset presents an appended column the reader fills with the file's path value as a constant.
 * {@link #syntheticLeaves} yields those appended schema leaves and {@link #constantsFor} the per-file constant output
 * columns; both agree with what {@link ConstantColumnBatches} produces for the same constant value.
 *
 * <p>A bound key whose path value cannot be parsed against its physical column type (an unparseable number, Hive's
 * null- partition sentinel, an unsupported physical kind) contributes no statistic; pruning simply does not apply to
 * that column for that file, and the catalog open still succeeds.
 */
public final class HivePartitioning {

    private final Map<String, BoundColumn> boundColumns;

    private HivePartitioning(Map<String, BoundColumn> boundColumns) {
        this.boundColumns = boundColumns;
    }

    /**
     * A bound partition key. A PHYSICAL key holds the schema column's physical {@code kind} and {@code logicalType},
     * both needed to parse path values. A SYNTHETIC key (path-only) holds the {@link PartitionKind} inferred from its
     * path values; {@code kind} and {@code logicalType} are unused.
     */
    private record BoundColumn(
            boolean synthetic, PrimitiveKind kind, Optional<LogicalType> logicalType, PartitionKind partitionKind) {}

    /** Whether any partition key was bound (the dataset is partitioned, physically or synthetically). */
    public boolean isPartitioned() {
        return !boundColumns.isEmpty();
    }

    /** Whether any partition key is synthetic (path-only, presented as an appended column). */
    public boolean hasSynthetic() {
        return boundColumns.values().stream().anyMatch(BoundColumn::synthetic);
    }

    /**
     * Binds the partition keys found across {@code perFilePartitions} against {@code schema}, in first-seen key order.
     * A key matching a top-level physical column binds physical; any other key binds synthetic from its path values.
     *
     * <p>A synthetic partition key is assumed present in every file of a uniform Hive tree, which the catalog's
     * schema-by-equality requirement implies. Non-uniform trees (a key present in some files only) are not supported in
     * this iteration.
     */
    public static HivePartitioning bind(List<Map<String, String>> perFilePartitions, ParquetSchema schema) {
        Objects.requireNonNull(perFilePartitions, "perFilePartitions");
        Objects.requireNonNull(schema, "schema");
        List<String> orderedKeys = orderedKeys(perFilePartitions);
        Map<String, BoundColumn> bound = new LinkedHashMap<>();
        for (String key : orderedKeys) {
            bound.put(key, bindKey(key, schema, perFilePartitions));
        }
        return new HivePartitioning(bound);
    }

    private static List<String> orderedKeys(List<Map<String, String>> perFilePartitions) {
        Set<String> keys = new LinkedHashSet<>();
        for (Map<String, String> partitions : perFilePartitions) {
            keys.addAll(partitions.keySet());
        }
        return new ArrayList<>(keys);
    }

    private static BoundColumn bindKey(String key, ParquetSchema schema, List<Map<String, String>> perFilePartitions) {
        Optional<SchemaNode> node = schema.find(ColumnPath.of(key));
        if (node.isPresent()) {
            return bindAgainstExistingColumn(key, node.get());
        }
        return bindSynthetic(key, perFilePartitions);
    }

    /**
     * Binds a partition key that names an existing top-level column. A non-repeated primitive binds physical. Any other
     * existing column (a group or a repeated primitive) cannot be a partition column, and appending a synthetic leaf of
     * the same name would yield two top-level columns named identically; this is rejected.
     */
    private static BoundColumn bindAgainstExistingColumn(String key, SchemaNode node) {
        if (isTopLevelPhysical(node)) {
            SchemaNode.Primitive primitive = (SchemaNode.Primitive) node;
            return new BoundColumn(false, primitive.kind(), primitive.logicalType(), null);
        }
        throw new IllegalStateException(
                "hive partition key '" + key + "' collides with a non-scalar column of the same name");
    }

    private static boolean isTopLevelPhysical(SchemaNode node) {
        return node instanceof SchemaNode.Primitive primitive && primitive.repetition() != Repetition.REPEATED;
    }

    private static BoundColumn bindSynthetic(String key, List<Map<String, String>> perFilePartitions) {
        List<String> values = valuesOf(key, perFilePartitions);
        rejectNullPartition(key, values);
        PartitionKind partitionKind = PartitionTypeInference.inferConsistent(values);
        return new BoundColumn(true, null, Optional.empty(), partitionKind);
    }

    /**
     * Typed-null path-only columns are not supported yet: a synthetic column inferred from path values has no physical
     * column whose type could give the null a meaning, and treating the sentinel as a literal string would silently
     * invert {@code IS NULL}. Fail fast instead.
     */
    private static void rejectNullPartition(String key, List<String> values) {
        for (String value : values) {
            if (PartitionValueParser.isNullPartition(value)) {
                throw new IllegalStateException("hive null partitions (" + PartitionValueParser.HIVE_NULL_PARTITION
                        + ") are not supported for path-only columns yet");
            }
        }
    }

    private static List<String> valuesOf(String key, List<Map<String, String>> perFilePartitions) {
        List<String> values = new ArrayList<>();
        for (Map<String, String> partitions : perFilePartitions) {
            String value = partitions.get(key);
            if (value != null) {
                values.add(value);
            }
        }
        return values;
    }

    /** The appended schema leaves for the synthetic keys, in key order. */
    public List<SchemaNode.Primitive> syntheticLeaves() {
        List<SchemaNode.Primitive> leaves = new ArrayList<>();
        for (Map.Entry<String, BoundColumn> entry : boundColumns.entrySet()) {
            if (entry.getValue().synthetic()) {
                leaves.add(syntheticLeaf(entry.getKey(), entry.getValue().partitionKind()));
            }
        }
        return leaves;
    }

    private static SchemaNode.Primitive syntheticLeaf(String key, PartitionKind partitionKind) {
        return ConstantLeaves.primitiveFor(key, representativeValue(partitionKind), syntheticFieldId());
    }

    /**
     * A constant {@link Value} of the same shape a synthetic column of this {@link PartitionKind} coerces its path
     * values to. Routing through it reaches the one {@link ConstantLeaves} mapping, which keeps a synthetic column's
     * advertised {@link FilesetDataset#schema()} leaf and the read-batch leaf agreeing on physical kind and logical
     * type. The value content is irrelevant here; only its shape selects the leaf type.
     */
    private static Value representativeValue(PartitionKind partitionKind) {
        return switch (partitionKind) {
            case LONG -> new Value.LongVal(0L);
            case DOUBLE -> new Value.DoubleVal(0.0);
            case DATE -> new Value.DateVal(LocalDate.EPOCH);
            case STRING -> new Value.StringVal("");
        };
    }

    /**
     * A field id for an appended synthetic leaf. Synthesized field ids are presentational only; column resolution is by
     * name, not by id, which keeps a constant id from colliding with how a leaf is later matched.
     */
    private static int syntheticFieldId() {
        return -1;
    }

    /**
     * Exact ({@code min == max}) pruning stats for one file from its path {@code partitionValues} and
     * {@code recordCount}. A physical value that does not parse against its bound column type contributes no stat; a
     * synthetic value contributes its coerced constant.
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
        Optional<Value> value = column.synthetic()
                ? Optional.of(coerce(column, rawValue))
                : PartitionValueParser.parse(rawValue, column.kind(), column.logicalType());
        value.ifPresent(parsed -> builder.column(
                ColumnPath.of(key),
                new ColumnStatistics(Optional.of(parsed), Optional.of(parsed), OptionalLong.of(0))));
    }

    /** The constant output columns for the synthetic keys present in {@code partitionValues}, in key order. */
    public List<ConstantColumn> constantsFor(Map<String, String> partitionValues) {
        List<ConstantColumn> constants = new ArrayList<>();
        for (Map.Entry<String, BoundColumn> entry : boundColumns.entrySet()) {
            String key = entry.getKey();
            BoundColumn column = entry.getValue();
            if (column.synthetic() && partitionValues.containsKey(key)) {
                Value value = coerce(column, partitionValues.get(key));
                constants.add(new ConstantColumn(ColumnPath.of(key), value));
            }
        }
        return constants;
    }

    /** The synthetic columns of {@code partitionValues} mapped to their coerced constant value. */
    public Map<ColumnPath, Value> constantMap(Map<String, String> partitionValues) {
        Map<ColumnPath, Value> constants = new LinkedHashMap<>();
        for (ConstantColumn constant : constantsFor(partitionValues)) {
            constants.put(constant.path(), constant.value());
        }
        return constants;
    }

    /** The paths of every synthetic column. */
    public Set<ColumnPath> syntheticPaths() {
        Set<ColumnPath> paths = new LinkedHashSet<>();
        for (Map.Entry<String, BoundColumn> entry : boundColumns.entrySet()) {
            if (entry.getValue().synthetic()) {
                paths.add(ColumnPath.of(entry.getKey()));
            }
        }
        return paths;
    }

    /**
     * The synthetic columns whose value is SQL null for this file. Always empty today: an odd or sentinel path value is
     * non-numeric, which degrades the whole column to STRING through inference and yields a literal string, never a
     * null. This is a reserved hook, not a limitation: it feeds the constant-folding null-column API ({@code nulls}),
     * the same machinery a future schema-evolution / Iceberg field-id read path reuses to inject a typed-null column
     * for a field that is absent from a file.
     */
    public Set<ColumnPath> nullColumns(Map<String, String> partitionValues) {
        Objects.requireNonNull(partitionValues, "partitionValues");
        return Set.of();
    }

    private static Value coerce(BoundColumn column, String rawValue) {
        return switch (column.partitionKind()) {
            case LONG -> new Value.LongVal(Long.parseLong(rawValue));
            case DOUBLE -> new Value.DoubleVal(Double.parseDouble(rawValue));
            case DATE -> new Value.DateVal(LocalDate.parse(rawValue));
            case STRING -> new Value.StringVal(rawValue);
        };
    }
}
