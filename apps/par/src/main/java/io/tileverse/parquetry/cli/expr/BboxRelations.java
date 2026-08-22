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
package io.tileverse.parquetry.cli.expr;

import io.tileverse.parquetry.cli.expr.SpatialFilterTranslator.Relation;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Predicate.Spatial;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Translates a spatial relation whose geometry operand is reduced to its bounding rectangle into one of the engine-free
 * bbox relations of {@link Predicate.Spatial}.
 *
 * <p>A relation translates only when that family has a relation answering it exactly on two rectangles, possibly
 * negated. With inclusive edges that holds for Intersects, Covers, CoveredBy and Equals, and for Disjoint as the
 * negation of Intersects. The interior-sensitive pair (Contains, Within) answers differently for a geometry lying on
 * the query edge; the family has no relation for Touches, Crosses or Overlaps, which two rectangles do answer between
 * themselves; an expanded box makes DWithin sound but not exact. Each of those is rejected with the exact-geometry
 * escape named in the message.
 */
final class BboxRelations {

    private BboxRelations() {}

    /**
     * A bounding-box relation request: the geometry column, the query rectangle, which side the column was written on,
     * the distance the call gave (only DWithin has one), and the extent call as the user spelled it, quoted back in
     * rejection messages.
     */
    record BoxCall(ColumnPath column, Bbox box, boolean columnFirst, double distance, String extentCall) {}

    /**
     * Maps a relation onto the bbox family. The switch is exhaustive with no default: a relation added later cannot
     * compile until it is classified here.
     */
    static Predicate translate(Relation relation, BoxCall call) {
        ColumnPath column = call.column();
        Bbox box = call.box();
        return switch (relation) {
            case ST_INTERSECTS -> new Spatial.BboxIntersects(column, box);
            case ST_EQUALS -> new Spatial.BboxEquals(column, box);
            case ST_DISJOINT -> Pred.not(new Spatial.BboxIntersects(column, box));
            case ST_COVERS ->
                call.columnFirst() ? new Spatial.BboxContains(column, box) : new Spatial.BboxCoveredBy(column, box);
            case ST_COVERED_BY ->
                call.columnFirst() ? new Spatial.BboxCoveredBy(column, box) : new Spatial.BboxContains(column, box);
            case ST_CONTAINS -> throw interiorSensitive(relation, "ST_Covers", call);
            case ST_WITHIN -> throw interiorSensitive(relation, "ST_CoveredBy", call);
            case ST_TOUCHES, ST_CROSSES, ST_OVERLAPS -> throw noBoxForm(relation, call);
            case ST_DWITHIN -> throw notExactOnBoxes(relation, call);
        };
    }

    private static FilterParseException interiorSensitive(Relation relation, String edgeInclusive, BoxCall call) {
        return new FilterParseException(relation.sqlName()
                + " has no bounding-box form: box relations include their edges, and a geometry on the query edge"
                + " answers differently under " + relation.sqlName()
                + "; use " + edgeInclusive + " for the box relation, or " + dropTheExtentCall(call));
    }

    private static FilterParseException noBoxForm(Relation relation, BoxCall call) {
        return new FilterParseException("the bounding-box predicates have no relation for " + relation.sqlName() + "; "
                + dropTheExtentCall(call));
    }

    private static FilterParseException notExactOnBoxes(Relation relation, BoxCall call) {
        return new FilterParseException(relation.sqlName()
                + " has no exact bounding-box form (an expanded box also matches rows farther than " + call.distance()
                + " near the corners); " + dropTheExtentCall(call));
    }

    /** The escape every rejection names: unwrap the column and the relation is answered against the geometry. */
    private static String dropTheExtentCall(BoxCall call) {
        return "remove " + call.extentCall() + " to test the exact geometry";
    }
}
