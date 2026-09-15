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
package io.tileverse.parquetry.geotools.data;

import static io.tileverse.parquetry.internal.filter.TemporalValues.fitsUnit;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Optional;

import org.geotools.api.filter.Filter;
import org.geotools.api.filter.MultiValuedFilter;
import org.geotools.util.Converters;

import io.tileverse.parquetry.filter.MatchAction;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * A column against which a comparison builds: its physical path, its Java binding for literal coercion, whether it is a
 * repeated (list/map element) leaf that an existential quantifier must aggregate over, and the physical kind plus
 * logical type annotation that decide how a decimal or temporal literal encodes. It also converts a temporal literal to
 * the value stored by the column, at the precision allowed by the column's own unit.
 */
record Leaf(
        ColumnPath path, Class<?> binding, boolean repeated, PrimitiveKind kind, Optional<LogicalType> logicalType) {

    /** Wraps {@code leaf} in the filter's match-action quantifier when this column is repeated; else returns it. */
    Predicate quantify(Predicate leaf, Filter filter) {
        if (repeated) {
            return new Predicate.Quantified(matchAction(filter), leaf);
        }
        return leaf;
    }

    /** Maps the GeoTools match action of a multi-valued filter to its parquetry equivalent; defaults to ANY. */
    private static MatchAction matchAction(Filter filter) {
        if (!(filter instanceof MultiValuedFilter multiValued)) {
            return MatchAction.ANY;
        }
        return switch (multiValued.getMatchAction()) {
            case ALL -> MatchAction.ALL;
            case ONE -> MatchAction.ONE;
            case ANY -> MatchAction.ANY;
        };
    }

    /**
     * The timestamp literal as the UTC wall-clock value stored by this column, or empty when the literal does not
     * convert or is finer than the column unit. A value finer than the unit is rejected by the engine rather than
     * truncated, hence such a comparison stays residual. A TIMESTAMP column outside INT64 is excluded too: a timestamp
     * attribute binds on an INT64 column alone.
     */
    Optional<LocalDateTime> timestampLiteral(Object rawValue) {
        LocalDateTime v = Converters.convert(rawValue, LocalDateTime.class);
        if (v == null || kind != PrimitiveKind.INT64) {
            return Optional.empty();
        }
        Optional<LogicalType.TimeUnit> unit = timestampUnit();
        if (unit.isEmpty() || !fitsUnit(v.getNano(), unit.get())) {
            return Optional.empty();
        }
        return Optional.of(v);
    }

    /** The unit of this TIMESTAMP column, or empty when the column has no timestamp annotation. */
    // S7475: Palantir formatter does not accept the bare `_` unnamed pattern; keep the typed unnamed binding.
    @SuppressWarnings("java:S7475")
    private Optional<LogicalType.TimeUnit> timestampUnit() {
        LogicalType annotation = logicalType.orElse(null);
        if (annotation instanceof LogicalType.Timestamp(boolean _, LogicalType.TimeUnit unit)) {
            return Optional.of(unit);
        }
        return Optional.empty();
    }

    /**
     * The time literal, or empty when it does not convert or is finer than the column unit. A TIME column outside INT64
     * is excluded too: the engine decodes a time cell from an INT64 count since midnight alone.
     */
    Optional<LocalTime> timeLiteral(Object rawValue) {
        LocalTime v = Converters.convert(rawValue, LocalTime.class);
        if (v == null || kind != PrimitiveKind.INT64) {
            return Optional.empty();
        }
        Optional<LogicalType.TimeUnit> unit = timeUnit();
        if (unit.isEmpty() || !fitsUnit(v.toNanoOfDay(), unit.get())) {
            return Optional.empty();
        }
        return Optional.of(v);
    }

    /** The unit of this TIME column, or empty when the column has no time annotation. */
    // S7475: Palantir formatter does not accept the bare `_` unnamed pattern; keep the typed unnamed binding.
    @SuppressWarnings("java:S7475")
    private Optional<LogicalType.TimeUnit> timeUnit() {
        LogicalType annotation = logicalType.orElse(null);
        if (annotation instanceof LogicalType.Time(boolean _, LogicalType.TimeUnit unit)) {
            return Optional.of(unit);
        }
        return Optional.empty();
    }
}
