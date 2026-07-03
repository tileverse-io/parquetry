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
package io.tileverse.parquetry.data;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import io.tileverse.parquetry.columnar.OutputBatches;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * The resolved projection for one read: the {@link Projection} the filter pipeline and decode run over (physical leaves
 * only), the physical output schema surviving batches are narrowed to, the produced result schema the read
 * materializes, and the produce step that shapes a decoded batch into the produce set. The projection-resolution logic
 * lives here, out of {@link ParquetFileReader}.
 *
 * <p>A produce set that names no physical column (only constants and nulls) still needs one decoded column to enumerate
 * the file's rows. {@link #resolve} keeps one such column in the decode and narrow sets - a predicate column when the
 * filter has one, else a cheap leaf - and {@link #produce} drops it from the produced result.
 */
final class ProjectionPlan {

    private final Projection scanProjection;
    private final ParquetSchema physicalOutputSchema;
    private final ParquetSchema producedSchema;
    private final Optional<Projection.Of> shaping;

    private ProjectionPlan(
            Projection scanProjection,
            ParquetSchema physicalOutputSchema,
            ParquetSchema producedSchema,
            Optional<Projection.Of> shaping) {
        this.scanProjection = scanProjection;
        this.physicalOutputSchema = physicalOutputSchema;
        this.producedSchema = producedSchema;
        this.shaping = shaping;
    }

    static ProjectionPlan resolve(ParquetSchema fileSchema, Projection projection, Predicate predicate) {
        return switch (projection) {
            case Projection.All _ -> new ProjectionPlan(Projection.ALL, fileSchema, fileSchema, Optional.empty());
            case Projection.Of of -> resolveOf(fileSchema, of, predicate);
        };
    }

    private static ProjectionPlan resolveOf(ParquetSchema fileSchema, Projection.Of of, Predicate predicate) {
        Set<ColumnPath> physical = of.physicalColumns();
        Set<ColumnPath> decodeSet = new LinkedHashSet<>(physical);
        decodeSet.addAll(Predicate.columns(predicate));
        // A predicate may reference synthesized row-position columns (a positional-delete leaf); the reader produces
        // those rather than decoding them, and ParquetSchema.project must never receive them.
        Predicate.rowPositionColumns(predicate).forEach(decodeSet::remove);
        if (decodeSet.isEmpty()) {
            decodeSet.add(fileSchema.leafColumns().get(0));
        }
        Set<ColumnPath> outputPhysical =
                physical.isEmpty() ? Set.of(decodeSet.iterator().next()) : physical;
        Projection scan = Projection.ofPhysical(decodeSet);
        ParquetSchema physicalOutput = fileSchema.project(outputPhysical).withAppendedLeaves(rowPositionLeaves(of));
        if (of.needsShaping()) {
            ParquetSchema produced = OutputBatches.producedSchema(fileSchema, of);
            return new ProjectionPlan(scan, physicalOutput, produced, Optional.of(of));
        }
        return new ProjectionPlan(scan, physicalOutput, physicalOutput, Optional.empty());
    }

    /**
     * The synthesized output leaves of {@code of}: its row-position outputs plus any coalesce presented under a name
     * other than its source (a coalesce under its source name reuses that physical leaf). The record-level filter
     * narrows a surviving batch to this schema; appending these leaves keeps the decode-time synthesized vectors
     * through that narrow step and into {@link #produce}. A produce set with no synthesized output appends nothing.
     */
    private static List<SchemaNode.Primitive> rowPositionLeaves(Projection.Of of) {
        List<SchemaNode.Primitive> leaves = new ArrayList<>();
        for (Projection.Column column : of.columns()) {
            switch (column) {
                case Projection.Column.RowPosition(ColumnPath name, long _) ->
                    leaves.add(OutputBatches.rowPositionLeaf(name.name(), -1));
                case Projection.Column.Coalesce(
                        ColumnPath name,
                        ColumnPath source,
                        Projection.Column.Coalesce.Fallback _) -> {
                    if (!name.equals(source)) {
                        leaves.add(OutputBatches.rowPositionLeaf(name.name(), -1));
                    }
                }
                default -> {
                    // physical, promoted, constant, and null columns synthesize nothing
                }
            }
        }
        return leaves;
    }

    /** The projection the filter pipeline and decode run over: physical leaves only. */
    Projection scanProjection() {
        return scanProjection;
    }

    /** The schema a surviving batch is narrowed to before the produce step: the physical output columns. */
    ParquetSchema physicalOutputSchema() {
        return physicalOutputSchema;
    }

    /** The schema the read materializes: the produced result schema (the physical output schema when no shaping). */
    ParquetSchema producedSchema() {
        return producedSchema;
    }

    /** True when producing the result needs shaping beyond the physical passthrough (rename, fill, widen, reorder). */
    boolean needsShaping() {
        return shaping.isPresent();
    }

    /** Shapes a decoded physical batch into the produce set, or returns it unchanged when no shaping is needed. */
    ParquetRecordBatch produce(ParquetRecordBatch base) {
        return shaping.map(of -> OutputBatches.produce(base, of, producedSchema))
                .orElse(base);
    }
}
