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
package io.tileverse.parquetry.iceberg;

import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.filter.prune.ColumnStatistics;
import io.tileverse.parquetry.filter.prune.FileStats;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.schema.ColumnPath;

/** Builds a core {@link FileStats} from an Iceberg data-file reference and the table's field list. */
final class IcebergFileStats {

    private IcebergFileStats() {}

    /**
     * Joins a data file's raw manifest bounds with the table schema. For each field, its field id keys into the
     * reference's bound maps and its Iceberg type decodes the bound bytes: a geometry column decodes to a
     * {@link BoundingBox}, every other column to a typed min/max. Decoding is best-effort; a bound that cannot be read
     * is skipped, which only makes pruning for that column less effective and never fails the read.
     *
     * <p>An identity-partition column that the writer omitted from the data file has no manifest bound, yet its value
     * is known exactly from the partition tuple. {@code partitionConstants} (keyed by source field id) provides those
     * values; each yields a tight {@code [v, v]} bound with a zero null count, letting the stats pruning tier skip a
     * file whose partition value cannot match the predicate before the file is opened.
     */
    public static FileStats from(
            IcebergManifests.DataFileRef ref, List<IcebergField> fields, Map<Integer, Value> partitionConstants) {
        FileStats.Builder builder = FileStats.builder().recordCount(ref.recordCount());
        for (IcebergField field : fields) {
            if (field.isGeometry()) {
                addGeometry(builder, field, ref);
            } else {
                addColumn(builder, field, ref);
            }
        }
        addPartitionBounds(builder, fields, partitionConstants);
        return builder.build();
    }

    private static void addPartitionBounds(
            FileStats.Builder builder, List<IcebergField> fields, Map<Integer, Value> partitionConstants) {
        for (IcebergField field : fields) {
            Value constant = partitionConstants.get(field.fieldId());
            if (constant == null) {
                continue;
            }
            builder.column(
                    ColumnPath.of(field.name()),
                    new ColumnStatistics(Optional.of(constant), Optional.of(constant), OptionalLong.of(0L)));
        }
    }

    private static void addGeometry(FileStats.Builder builder, IcebergField field, IcebergManifests.DataFileRef ref) {
        MemorySegment lower = ref.lowerBounds().get(field.fieldId());
        MemorySegment upper = ref.upperBounds().get(field.fieldId());
        if (lower == null || upper == null) {
            return;
        }
        BoundingBox box = decodeBounds(lower, upper);
        if (box == null) {
            return;
        }
        builder.geometryBounds(ColumnPath.of(field.name()), box);
    }

    private static BoundingBox decodeBounds(MemorySegment lower, MemorySegment upper) {
        try {
            double[] min = IcebergBounds.decodePoint(lower);
            double[] max = IcebergBounds.decodePoint(upper);
            return new BoundingBox(
                    min[0],
                    max[0],
                    min[1],
                    max[1],
                    OptionalDouble.empty(),
                    OptionalDouble.empty(),
                    OptionalDouble.empty(),
                    OptionalDouble.empty());
        } catch (IcebergFormatException undecodable) {
            // Any bound we cannot decode (an unsupported encoding or a malformed payload) is skipped: pruning is
            // best-effort and never blocks a read. A bad geometry bound just means no spatial pruning for this column.
            return null;
        }
    }

    private static void addColumn(FileStats.Builder builder, IcebergField field, IcebergManifests.DataFileRef ref) {
        MemorySegment lower = ref.lowerBounds().get(field.fieldId());
        MemorySegment upper = ref.upperBounds().get(field.fieldId());
        Optional<Value> min = lower == null ? Optional.empty() : decode(field, lower);
        Optional<Value> max = upper == null ? Optional.empty() : decode(field, upper);
        Long nulls = ref.nullValueCounts().get(field.fieldId());
        OptionalLong nullCount = nulls == null ? OptionalLong.empty() : OptionalLong.of(nulls);
        if (min.isEmpty() && max.isEmpty() && nullCount.isEmpty()) {
            return;
        }
        builder.column(ColumnPath.of(field.name()), new ColumnStatistics(min, max, nullCount));
    }

    private static Optional<Value> decode(IcebergField field, MemorySegment bytes) {
        try {
            return Optional.of(IcebergBounds.decodeValue(field.type(), bytes));
        } catch (IcebergFormatException undecodable) {
            // Any bound we cannot decode (an unsupported type or a malformed payload) is skipped: pruning is
            // best-effort and never blocks a read. The column simply contributes nothing to pruning.
            return Optional.empty();
        }
    }
}
