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
package io.tileverse.parquetry.filter.prune;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.internal.filter.ColumnSummary;
import io.tileverse.parquetry.internal.filter.NaNCells;
import io.tileverse.parquetry.internal.filter.PredicateNormalizer;
import io.tileverse.parquetry.internal.filter.SpatialBoundsEvaluator;
import io.tileverse.parquetry.internal.filter.StatsEvaluator;
import io.tileverse.parquetry.internal.filter.spatial.SuppliedBoundsSource;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.ParquetSchemaException;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaNode;

import lombok.NonNull;

/**
 * Decides whether the files of a dataset can be skipped for one query, given the statistics of each file. Pure
 * work-avoidance: a kept file is still filtered at row-group and record level during the read; an eliminated file would
 * have produced no matching rows. A geometry box wrapping the antimeridian prunes on its y extent alone, as the
 * row-group SPATIAL tier does.
 *
 * <p>The pruner checks its predicate against the dataset schema as a read does, and takes the type of each column from
 * that schema. Statistics cannot tell whether a query is valid. Without the check, the pruner would drop each file
 * holding only nulls in a column compared with a literal of the wrong type, and the query would return no rows where a
 * read throws.
 */
public final class FilePruner {

    private final Predicate normalized;
    private final Map<ColumnPath, PrimitiveKind> kinds;

    private FilePruner(Predicate normalized, Map<ColumnPath, PrimitiveKind> kinds) {
        this.normalized = normalized;
        this.kinds = kinds;
    }

    /**
     * A pruner for {@code predicate} over the files of a dataset whose schema is {@code schema}.
     *
     * @throws ParquetSchemaException when the predicate does not fit {@code schema}, as a read would throw
     */
    public static FilePruner of(@NonNull Predicate predicate, @NonNull ParquetSchema schema) {
        Predicate normalized = PredicateNormalizer.normalizeAndValidate(predicate, schema);
        return new FilePruner(normalized, kindsOf(normalized, schema));
    }

    /** The physical kind of each column of {@code predicate} that names a leaf of {@code schema}. */
    private static Map<ColumnPath, PrimitiveKind> kindsOf(Predicate predicate, ParquetSchema schema) {
        Map<ColumnPath, PrimitiveKind> kinds = new HashMap<>();
        for (ColumnPath column : Predicate.columns(predicate)) {
            Optional<SchemaNode> node = schema.find(column);
            if (node.isPresent() && node.orElseThrow() instanceof SchemaNode.Primitive primitive) {
                kinds.put(column, primitive.kind());
            }
        }
        return kinds;
    }

    /**
     * Eliminated when the numeric or spatial statistics of a file prove no row can match; otherwise a keep decision.
     */
    public PruningDecision evaluate(@NonNull FileStats stats) {
        PruningDecision numeric = StatsEvaluator.evaluate(normalized, typedColumns(stats), stats.recordCount());
        if (numeric instanceof PruningDecision.Eliminated) {
            return numeric;
        }
        SuppliedBoundsSource bounds = new SuppliedBoundsSource(stats.geometryBounds());
        PruningDecision spatial = SpatialBoundsEvaluator.evaluate(normalized, bounds, 0);
        if (spatial instanceof PruningDecision.Eliminated) {
            return spatial;
        }
        return numeric;
    }

    private StatsEvaluator.TypedColumns typedColumns(FileStats stats) {
        Map<ColumnPath, ColumnStatistics> columns = stats.columns();
        return path -> {
            ColumnStatistics statistics = columns.get(path);
            PrimitiveKind kind = kinds.get(path);
            if (statistics == null || kind == null) {
                return Optional.empty();
            }
            return Optional.of(summary(kind, statistics));
        };
    }

    private static ColumnSummary summary(PrimitiveKind kind, ColumnStatistics statistics) {
        return ColumnSummary.of(
                kind,
                statistics.min(),
                statistics.max(),
                statistics.nullCount(),
                statistics.valueCount(),
                NaNCells.POSSIBLE);
    }
}
