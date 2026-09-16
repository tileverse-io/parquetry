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

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.Optional;

import org.geotools.api.filter.temporal.After;
import org.geotools.api.filter.temporal.AnyInteracts;
import org.geotools.api.filter.temporal.Before;
import org.geotools.api.filter.temporal.Begins;
import org.geotools.api.filter.temporal.BegunBy;
import org.geotools.api.filter.temporal.BinaryTemporalOperator;
import org.geotools.api.filter.temporal.During;
import org.geotools.api.filter.temporal.EndedBy;
import org.geotools.api.filter.temporal.Ends;
import org.geotools.api.filter.temporal.Meets;
import org.geotools.api.filter.temporal.MetBy;
import org.geotools.api.filter.temporal.OverlappedBy;
import org.geotools.api.filter.temporal.TContains;
import org.geotools.api.filter.temporal.TEquals;
import org.geotools.api.filter.temporal.TOverlaps;
import org.geotools.api.temporal.Period;

import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * The reduction of a GeoTools temporal operator on a timestamp attribute to comparisons on that attribute. It follows
 * the position rules by which GeoTools evaluates the operator: an attribute value is an instant, and against an instant
 * literal or the bounds of a period literal each operator either compares the attribute with one bound or can never
 * hold. The attribute may sit on either side of the operator.
 */
final class TemporalPredicates {

    private TemporalPredicates() {}

    /**
     * The comparisons on {@code column} for which {@code f} holds against the literal {@code rawValue}, or empty when
     * the operator stays in the residual filter. {@code attributeFirst} tells whether the attribute is the first
     * operand of {@code f}.
     *
     * <p>Three cases are left to the residual filter. A date or a time attribute, because GeoTools evaluates these
     * operators on instants while such an attribute converts to a date or a time. A repeated (list or map element)
     * leaf, because GeoTools evaluates a temporal operator on a list-valued attribute as false: its evaluation ignores
     * the match action, and a list converts to no temporal primitive. Pushing a per-element reading would answer a
     * question that the caller did not ask. An operator outside the fourteen types declared by
     * {@link PushdownFilterCapabilities}, whose position rules are unknown here.
     */
    static Optional<Predicate> reduce(BinaryTemporalOperator f, boolean attributeFirst, Leaf column, Object rawValue) {
        if (!isSingleValuedTimestamp(column)) {
            return Optional.empty();
        }
        if (rawValue instanceof Period period) {
            Optional<PeriodRelation> relation = periodRelation(f, attributeFirst);
            if (relation.isEmpty()) {
                return Optional.empty();
            }
            return relation.get().toPredicate(column, period);
        }
        Optional<LocalDateTime> at = column.timestampLiteral(rawValue);
        if (at.isEmpty()) {
            return Optional.empty();
        }
        return instantRelation(f, attributeFirst, column, at.get());
    }

    /**
     * True when the column holds one timestamp per row, which is the only attribute shape on which these operators
     * reduce.
     */
    private static boolean isSingleValuedTimestamp(Leaf column) {
        Class<?> binding = column.binding();
        boolean timestamp = binding == Instant.class || binding == LocalDateTime.class;
        return timestamp && !column.repeated();
    }

    /**
     * The comparison for {@code f} against an instant literal, matching no row in a position never taken by an instant,
     * or empty for an operator outside the fourteen temporal types.
     */
    private static Optional<Predicate> instantRelation(
            BinaryTemporalOperator f, boolean attributeFirst, Leaf column, LocalDateTime at) {
        ColumnPath path = column.path();
        Value.TimestampVal value = new Value.TimestampVal(at, column.binding() == Instant.class);
        return switch (f) {
            case After _ -> Optional.of(attributeFirst ? new Predicate.Gt(path, value) : new Predicate.Lt(path, value));
            case Before _ ->
                Optional.of(attributeFirst ? new Predicate.Lt(path, value) : new Predicate.Gt(path, value));
            case TEquals _, AnyInteracts _ -> Optional.of(new Predicate.Eq(path, value));
            case Begins _,
                    BegunBy _,
                    During _,
                    EndedBy _,
                    Ends _,
                    Meets _,
                    MetBy _,
                    OverlappedBy _,
                    TContains _,
                    TOverlaps _ -> Optional.of(Predicate.ALWAYS_FALSE);
            default -> Optional.empty();
        };
    }

    /**
     * How an instant attribute must relate to the bounds of a period literal for {@code f} to hold, or empty for an
     * operator outside the fourteen temporal types.
     */
    private static Optional<PeriodRelation> periodRelation(BinaryTemporalOperator f, boolean attributeFirst) {
        if (attributeFirst) {
            return attributeFirstRelation(f);
        }
        return literalFirstRelation(f);
    }

    private static Optional<PeriodRelation> attributeFirstRelation(BinaryTemporalOperator f) {
        return switch (f) {
            case After _ -> Optional.of(PeriodRelation.AFTER_END);
            case Before _ -> Optional.of(PeriodRelation.BEFORE_BEGIN);
            case During _ -> Optional.of(PeriodRelation.STRICTLY_INSIDE);
            case Begins _ -> Optional.of(PeriodRelation.AT_BEGIN);
            case Ends _ -> Optional.of(PeriodRelation.AT_END);
            case AnyInteracts _ -> Optional.of(PeriodRelation.INSIDE_OR_AT_BOUNDS);
            case BegunBy _, EndedBy _, Meets _, MetBy _, OverlappedBy _, TContains _, TEquals _, TOverlaps _ ->
                Optional.of(PeriodRelation.NEVER);
            default -> Optional.empty();
        };
    }

    private static Optional<PeriodRelation> literalFirstRelation(BinaryTemporalOperator f) {
        return switch (f) {
            case After _ -> Optional.of(PeriodRelation.BEFORE_BEGIN);
            case Before _ -> Optional.of(PeriodRelation.AFTER_END);
            case TContains _ -> Optional.of(PeriodRelation.STRICTLY_INSIDE);
            case BegunBy _ -> Optional.of(PeriodRelation.AT_BEGIN);
            case EndedBy _ -> Optional.of(PeriodRelation.AT_END);
            case AnyInteracts _ -> Optional.of(PeriodRelation.INSIDE_OR_AT_BOUNDS);
            case Begins _, During _, Ends _, Meets _, MetBy _, OverlappedBy _, TEquals _, TOverlaps _ ->
                Optional.of(PeriodRelation.NEVER);
            default -> Optional.empty();
        };
    }

    /** The relation of an instant attribute to a period literal, as comparisons against the period's bounds. */
    private enum PeriodRelation {
        AFTER_END,
        BEFORE_BEGIN,
        STRICTLY_INSIDE,
        AT_BEGIN,
        AT_END,
        INSIDE_OR_AT_BOUNDS,
        NEVER;

        /** The comparisons on {@code column} for which this relation holds, or empty when a bound does not convert. */
        Optional<Predicate> toPredicate(Leaf column, Period period) {
            if (this == NEVER) {
                return Optional.of(Predicate.ALWAYS_FALSE);
            }
            Optional<LocalDateTime> beginAt = boundLiteral(column, period.getBeginning());
            Optional<LocalDateTime> endAt = boundLiteral(column, period.getEnding());
            if (beginAt.isEmpty() || endAt.isEmpty()) {
                return Optional.empty();
            }
            boolean adjustedToUtc = column.binding() == Instant.class;
            Value.TimestampVal begin = new Value.TimestampVal(beginAt.get(), adjustedToUtc);
            Value.TimestampVal end = new Value.TimestampVal(endAt.get(), adjustedToUtc);
            ColumnPath path = column.path();
            return Optional.of(
                    switch (this) {
                        case AFTER_END -> new Predicate.Gt(path, end);
                        case BEFORE_BEGIN -> new Predicate.Lt(path, begin);
                        case STRICTLY_INSIDE -> Pred.and(new Predicate.Gt(path, begin), new Predicate.Lt(path, end));
                        case AT_BEGIN -> new Predicate.Eq(path, begin);
                        case AT_END -> new Predicate.Eq(path, end);
                        case INSIDE_OR_AT_BOUNDS ->
                            Pred.and(new Predicate.GtEq(path, begin), new Predicate.LtEq(path, end));
                        case NEVER -> Predicate.ALWAYS_FALSE;
                    });
        }

        /**
         * A period bound as the UTC wall-clock value stored by {@code column}, or empty when the bound is absent or
         * holds a position with no date reading. A period literal of any shape leaves the translator without throwing.
         */
        private static Optional<LocalDateTime> boundLiteral(Leaf column, org.geotools.api.temporal.Instant bound) {
            if (bound == null || bound.getPosition() == null) {
                return Optional.empty();
            }
            Date date = bound.getPosition().getDate();
            return column.timestampLiteral(date);
        }
    }
}
