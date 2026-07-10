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

import java.util.Map;
import java.util.Optional;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.internal.filter.PredicateNormalizer;
import io.tileverse.parquetry.internal.filter.SpatialBoundsEvaluator;
import io.tileverse.parquetry.internal.filter.StatsEvaluator;
import io.tileverse.parquetry.internal.filter.spatial.SuppliedBoundsSource;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * Decides whether a whole file can be skipped for a predicate, given the file's statistics. Pure work-avoidance: a kept
 * file is still filtered at row-group and record level during the read; an eliminated file would have produced no
 * matching rows. Geometry bounds that wrap the antimeridian are kept conservatively.
 */
public final class FilePruner {

    private FilePruner() {}

    /** Eliminated when the numeric or spatial statistics prove no row can match; otherwise a keep decision. */
    public static PruningDecision evaluate(Predicate predicate, FileStats stats) {
        Predicate normalized = PredicateNormalizer.normalize(predicate);

        PruningDecision numeric = StatsEvaluator.evaluate(normalized, typedColumns(stats), stats.recordCount());
        if (numeric instanceof PruningDecision.Eliminated) {
            return numeric;
        }
        if (hasWrappedBounds(stats)) {
            return numeric;
        }
        SuppliedBoundsSource bounds = new SuppliedBoundsSource(stats.geometryBounds());
        PruningDecision spatial = SpatialBoundsEvaluator.evaluate(normalized, bounds, 0);
        if (spatial instanceof PruningDecision.Eliminated) {
            return spatial;
        }
        return numeric;
    }

    private static StatsEvaluator.TypedColumns typedColumns(FileStats stats) {
        Map<ColumnPath, ColumnStatistics> columns = stats.columns();
        return path -> Optional.ofNullable(columns.get(path)).map(FilePruner::summary);
    }

    private static StatsEvaluator.ColumnSummary summary(ColumnStatistics c) {
        PrimitiveKind kind = c.min().or(c::max).map(FilePruner::kindOf).orElse(PrimitiveKind.BYTE_ARRAY);
        return new StatsEvaluator.ColumnSummary(kind, c.min(), c.max(), c.nullCount());
    }

    private static boolean hasWrappedBounds(FileStats stats) {
        for (BoundingBox box : stats.geometryBounds().values()) {
            if (box.wrapsAntimeridian()) {
                return true;
            }
        }
        return false;
    }

    private static PrimitiveKind kindOf(Value value) {
        return switch (value) {
            case Value.BoolVal _ -> PrimitiveKind.BOOLEAN;
            case Value.IntVal _ -> PrimitiveKind.INT32;
            case Value.LongVal _ -> PrimitiveKind.INT64;
            case Value.FloatVal _ -> PrimitiveKind.FLOAT;
            case Value.DoubleVal _ -> PrimitiveKind.DOUBLE;
            case Value.BinaryVal _ -> PrimitiveKind.BYTE_ARRAY;
            case Value.StringVal _ -> PrimitiveKind.BYTE_ARRAY;
            case Value.DateVal _ -> PrimitiveKind.INT32;
            case Value.TimestampVal _ -> PrimitiveKind.INT64;
            case Value.UuidVal _ -> PrimitiveKind.FIXED_LEN_BYTE_ARRAY;
        };
    }
}
