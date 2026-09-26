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
package io.tileverse.parquetry.internal.filter.spatial;

import java.util.List;

import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.GeometryFilter;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Decides from a unit's bounding box whether every non-null geometry of the unit satisfies a normalized predicate. A
 * unit is a row group or a page whose box encloses every geometry in it. {@code Always(true)} accepts; {@code And}
 * accepts when every child accepts and {@code Or} when any child accepts; a bbox-intersects or bbox-covered-by leaf on
 * the primary geometry column accepts a unit whose box lies within the query box (edges inclusive, Z ignored); an exact
 * geometry filter on that column accepts what its {@link GeometryFilter#coversRegion} answer covers. Every other shape,
 * attribute comparisons included, answers false: such a unit is tested row by row.
 *
 * <p>A unit box without a real extent on both axes proves nothing and is accepted by no shape at all. That rejects a
 * box wrapping the antimeridian, encoded with its minimum east of its maximum, whose inverted interval would otherwise
 * nest inside a query box and wrongly answer true.
 *
 * <p>A null geometry has no spatial truth value and passes no spatial leaf. Acceptance is therefore a statement about
 * the unit's non-null geometries: every one of them satisfies the predicate. A unit box comes from statistics computed
 * over non-null values, hence a unit with a box holds at least one such geometry, which is all that a caller emitting
 * one substitute for the whole unit needs. An empty geometry has no envelope and no covering values: it takes no part
 * in the box and fails every spatial leaf row by row, which leaves a substitute emitted from the box alone unaffected.
 *
 * <p>The predicate is the one supplied by the caller, normalized, before any covering rewrite. The covering-column
 * comparisons added by a rewrite are necessary conditions of their originating spatial leaf, and a unit accepted on
 * that leaf satisfies them.
 */
public final class UnitAcceptance {

    private UnitAcceptance() {}

    /**
     * True when every non-null geometry of a unit whose 2D bounds are {@code unitBox} satisfies {@code normalized}. A
     * false answer leaves the question open: the unit is tested row by row.
     *
     * @param normalized the predicate in normalized form, before any covering rewrite
     * @param primaryGeometry the geometry column bounded by {@code unitBox}
     * @param unitBox the box enclosing every geometry of the unit
     */
    public static boolean accepts(Predicate normalized, ColumnPath primaryGeometry, Bbox unitBox) {
        if (!hasRealExtent(unitBox)) {
            return false;
        }
        return switch (normalized) {
            case Predicate.Always(boolean value) -> value;
            case Predicate.And(List<Predicate> children) -> allAccept(children, primaryGeometry, unitBox);
            case Predicate.Or(List<Predicate> children) -> anyAccepts(children, primaryGeometry, unitBox);
            case Predicate.Spatial.BboxIntersects(ColumnPath col, Bbox query) ->
                col.equals(primaryGeometry) && unitBox.coveredBy(query);
            case Predicate.Spatial.BboxCoveredBy(ColumnPath col, Bbox query) ->
                col.equals(primaryGeometry) && unitBox.coveredBy(query);
            case Predicate.GeometryFilterPredicate(GeometryFilter<?> filter) ->
                filter.column().equals(primaryGeometry) && coversUnit(filter, unitBox);
            default -> false;
        };
    }

    /**
     * Whether {@code box} has a real extent on both axes: each minimum at or below its maximum. A box that wraps the
     * antimeridian is encoded with its minimum east of its maximum and fails this test, as does a NaN bound.
     */
    private static boolean hasRealExtent(Bbox box) {
        return box.minX() <= box.maxX() && box.minY() <= box.maxY();
    }

    private static boolean allAccept(List<Predicate> children, ColumnPath primaryGeometry, Bbox unitBox) {
        for (Predicate child : children) {
            if (!accepts(child, primaryGeometry, unitBox)) {
                return false;
            }
        }
        return true;
    }

    private static boolean anyAccepts(List<Predicate> children, ColumnPath primaryGeometry, Bbox unitBox) {
        for (Predicate child : children) {
            if (accepts(child, primaryGeometry, unitBox)) {
                return true;
            }
        }
        return false;
    }

    private static boolean coversUnit(GeometryFilter<?> filter, Bbox unitBox) {
        return filter.coversRegion(unitBox.minX(), unitBox.minY(), unitBox.maxX(), unitBox.maxY());
    }
}
