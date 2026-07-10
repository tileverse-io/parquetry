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
package io.tileverse.parquetry.internal.filter;

import java.util.List;
import java.util.Map;

import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.MatchAction;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Rewrites the leaf column paths of a {@link Predicate} through a column mapping.
 *
 * <p>A read presents its result under output names that may differ from the file's physical column names (a rename).
 * When a caller builds a predicate over those output names, the file-level pushdown must see the physical names
 * instead, because the filter pipeline validates every referenced column against the file schema and rejects names it
 * does not find. This helper lowers an output-named predicate to a file-physical one before pushdown.
 *
 * <p>A leaf column that is a key of the mapping is replaced with its mapped physical source; a leaf column absent from
 * the mapping is left unchanged (it is assumed already file-physical). The structural shape is preserved exactly; only
 * leaf column paths change.
 */
public final class PredicateColumns {

    private PredicateColumns() {}

    /**
     * Returns {@code predicate} with every leaf column path that is a key of {@code mapping} replaced by its mapped
     * value. Leaf columns absent from {@code mapping} are left unchanged. The predicate's structure and all non-column
     * fields are preserved.
     */
    public static Predicate remap(Predicate predicate, Map<ColumnPath, ColumnPath> mapping) {
        return switch (predicate) {
            case Predicate.Always _ -> predicate;
            case Predicate.And(List<Predicate> children) -> new Predicate.And(remapAll(children, mapping));
            case Predicate.Or(List<Predicate> children) -> new Predicate.Or(remapAll(children, mapping));
            case Predicate.Not(Predicate child) -> new Predicate.Not(remap(child, mapping));
            case Predicate.Eq(ColumnPath col, Value v) -> new Predicate.Eq(target(col, mapping), v);
            case Predicate.NotEq(ColumnPath col, Value v) -> new Predicate.NotEq(target(col, mapping), v);
            case Predicate.Lt(ColumnPath col, Value v) -> new Predicate.Lt(target(col, mapping), v);
            case Predicate.LtEq(ColumnPath col, Value v) -> new Predicate.LtEq(target(col, mapping), v);
            case Predicate.Gt(ColumnPath col, Value v) -> new Predicate.Gt(target(col, mapping), v);
            case Predicate.GtEq(ColumnPath col, Value v) -> new Predicate.GtEq(target(col, mapping), v);
            case Predicate.In(ColumnPath col, List<Value> values) -> new Predicate.In(target(col, mapping), values);
            case Predicate.IsNull(ColumnPath col) -> new Predicate.IsNull(target(col, mapping));
            case Predicate.IsNotNull(ColumnPath col) -> new Predicate.IsNotNull(target(col, mapping));
            case Predicate.Spatial spatial -> remapSpatial(spatial, mapping);
            case Predicate.GeometryFilterPredicate gfp -> remapGeometryFilter(gfp, mapping);
            // The row-position column is synthesized, not a physical file leaf; a physical-name remap leaves it as is.
            case Predicate.RowIndexExcluded _ -> predicate;
            case Predicate.Quantified(MatchAction match, Predicate leaf) ->
                new Predicate.Quantified(match, remap(leaf, mapping));
        };
    }

    private static List<Predicate> remapAll(List<Predicate> children, Map<ColumnPath, ColumnPath> mapping) {
        return children.stream().map(child -> remap(child, mapping)).toList();
    }

    private static Predicate.Spatial remapSpatial(Predicate.Spatial spatial, Map<ColumnPath, ColumnPath> mapping) {
        ColumnPath col = target(spatial.col(), mapping);
        Bbox bbox = spatial.bbox();
        return switch (spatial) {
            case Predicate.Spatial.BboxIntersects _ -> new Predicate.Spatial.BboxIntersects(col, bbox);
            case Predicate.Spatial.BboxContains _ -> new Predicate.Spatial.BboxContains(col, bbox);
            case Predicate.Spatial.BboxCoveredBy _ -> new Predicate.Spatial.BboxCoveredBy(col, bbox);
            case Predicate.Spatial.BboxEquals _ -> new Predicate.Spatial.BboxEquals(col, bbox);
        };
    }

    /**
     * A {@link Predicate.GeometryFilterPredicate} wraps an opaque {@link io.tileverse.parquetry.filter.GeometryFilter}
     * with no way to rebind its column; it can only pass through unchanged. A mapping that would actually move its
     * column to a different physical name is rejected loudly rather than silently filtering the wrong column.
     */
    private static Predicate remapGeometryFilter(
            Predicate.GeometryFilterPredicate gfp, Map<ColumnPath, ColumnPath> mapping) {
        ColumnPath column = gfp.filter().column();
        ColumnPath mapped = target(column, mapping);
        if (!mapped.equals(column)) {
            throw new IllegalArgumentException("Cannot lower a geometry filter on output column " + column.dot()
                    + " to physical column " + mapped.dot() + "; the geometry filter column is not rebindable");
        }
        return gfp;
    }

    private static ColumnPath target(ColumnPath col, Map<ColumnPath, ColumnPath> mapping) {
        return mapping.getOrDefault(col, col);
    }
}
