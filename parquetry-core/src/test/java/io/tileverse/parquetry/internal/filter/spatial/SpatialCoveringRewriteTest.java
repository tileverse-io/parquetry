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

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.GeometryFilter;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.geo.geoparquet.BboxCovering;
import io.tileverse.parquetry.schema.geo.geoparquet.Covering;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoColumn;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

class SpatialCoveringRewriteTest {

    private static final ColumnPath XMIN = ColumnPath.of("xmin");
    private static final ColumnPath XMAX = ColumnPath.of("xmax");
    private static final ColumnPath YMIN = ColumnPath.of("ymin");
    private static final ColumnPath YMAX = ColumnPath.of("ymax");

    private final ParquetSchema schema = flatSchema(
            requiredBinary("geometry"),
            requiredDouble("xmin"),
            requiredDouble("xmax"),
            requiredDouble("ymin"),
            requiredDouble("ymax"));

    private final ParquetSchema schemaWithBboxStruct = schemaWithBboxStruct();

    private final Optional<GeoParquetMetadata> geoWithCovering = Optional.of(geoWithCovering());

    /**
     * The covering encloses each row's geometry and may be wider than its bounding box, hence a relation lowers to the
     * strongest comparison that every matching row still satisfies once the box grows: overlap for the relations that
     * only need contact, enclosure for the ones that need the query box inside. Being enclosed BY the query, and
     * equality, do not survive a wider box and take the weaker relation they imply.
     */
    static Stream<Arguments> relationLowerings() {
        Bbox query = Bbox.of2d(0, 0, 10, 10);
        return Stream.of(
                Arguments.of("intersects", Pred.col("geometry").bboxIntersects(query), coveringOverlapsQuery()),
                Arguments.of("coveredBy", Pred.col("geometry").bboxCoveredBy(query), coveringOverlapsQuery()),
                Arguments.of("contains", Pred.col("geometry").bboxContains(query), coveringEnclosesQuery()),
                Arguments.of("equals", Pred.col("geometry").bboxEquals(query), coveringEnclosesQuery()));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("relationLowerings")
    void relationGainsCoveringComparisonsAndKeepsItsLeaf(
            String relation, Predicate spatial, Predicate expectedCovering) {

        Predicate rewritten = SpatialCoveringRewrite.expand(spatial, schema, geoWithCovering);

        assertThat(rewritten)
                .as("%s must prune on the covering columns and still decide each row from its geometry", relation)
                .isEqualTo(new Predicate.And(List.of(expectedCovering, spatial)));
    }

    @Test
    void noCoveringLeavesSpatialUnchanged() {
        Predicate spatial = Pred.col("geometry").bboxIntersects(Bbox.of2d(0, 0, 10, 10));
        Optional<GeoParquetMetadata> geoNoCovering = Optional.of(geoWithoutCovering());

        Predicate rewritten = SpatialCoveringRewrite.expand(spatial, schema, geoNoCovering);

        assertThat(rewritten).isSameAs(spatial);
    }

    @Test
    void noFormalCoveringFallsBackToConventionalBboxStruct() {
        Predicate spatial = Pred.col("geometry").bboxIntersects(Bbox.of2d(0, 0, 10, 10));
        Optional<GeoParquetMetadata> geoNoCovering = Optional.of(geoWithoutCovering());

        Predicate rewritten = SpatialCoveringRewrite.expand(spatial, schemaWithBboxStruct, geoNoCovering);

        Predicate expectedCovering = new Predicate.And(List.of(
                Pred.col(ColumnPath.of("bbox", "xmin")).ltEq(10.0),
                Pred.col(ColumnPath.of("bbox", "xmax")).gtEq(0.0),
                Pred.col(ColumnPath.of("bbox", "ymin")).ltEq(10.0),
                Pred.col(ColumnPath.of("bbox", "ymax")).gtEq(0.0)));
        assertThat(rewritten).isEqualTo(new Predicate.And(List.of(expectedCovering, spatial)));
    }

    @Test
    void emptyGeoLeavesSpatialUnchanged() {
        Predicate spatial = Pred.col("geometry").bboxIntersects(Bbox.of2d(0, 0, 10, 10));

        Predicate rewritten = SpatialCoveringRewrite.expand(spatial, schema, Optional.empty());

        assertThat(rewritten).isSameAs(spatial);
    }

    @Test
    void nonSpatialPredicateLeftUnchanged() {
        Predicate idEq = Pred.col("id").eq(1);

        Predicate rewritten = SpatialCoveringRewrite.expand(idEq, schema, geoWithCovering);

        assertThat(rewritten).isEqualTo(idEq);
    }

    @Test
    void nestedAndRewritesOnlyTheSpatialChild() {
        Predicate idEq = Pred.col("id").eq(1);
        Predicate spatial = Pred.col("geometry").bboxIntersects(Bbox.of2d(0, 0, 10, 10));
        Predicate nested = new Predicate.And(List.of(spatial, idEq));

        Predicate rewritten = SpatialCoveringRewrite.expand(nested, schema, geoWithCovering);

        Predicate expectedSpatial = new Predicate.And(List.of(coveringOverlapsQuery(), spatial));
        Predicate expected = new Predicate.And(List.of(expectedSpatial, idEq));
        assertThat(rewritten).isEqualTo(expected);
    }

    @Test
    void geometryFilterWithPresentLoweringAndCoveringProducesAndWithGatePreserved() {
        ColumnPath geomCol = ColumnPath.of("geometry");
        Bbox queryBox = Bbox.of2d(0, 0, 10, 10);
        Predicate.Spatial lowering = new Predicate.Spatial.BboxIntersects(geomCol, queryBox);
        Predicate.GeometryFilterPredicate gfp =
                new Predicate.GeometryFilterPredicate(fakePruningFilter(geomCol, Optional.of(lowering)));

        Predicate rewritten = SpatialCoveringRewrite.expand(gfp, schema, geoWithCovering);

        assertThat(rewritten)
                .as("result must be an And when a covering is available")
                .isInstanceOf(Predicate.And.class);
        Set<ColumnPath> cols = Predicate.columns(rewritten);
        assertThat(cols)
                .as("result must reference both covering columns and the geometry column")
                .contains(XMIN, XMAX, YMIN, YMAX, geomCol);
        assertThat(containsGeometryFilterPredicate(rewritten))
                .as("the GeometryFilterPredicate gate must still be present in the rewritten tree")
                .isTrue();
    }

    @Test
    void geometryFilterWithEmptyLoweringIsReturnedUnchanged() {
        ColumnPath geomCol = ColumnPath.of("geometry");
        Predicate.GeometryFilterPredicate gfp =
                new Predicate.GeometryFilterPredicate(fakePruningFilter(geomCol, Optional.empty()));

        Predicate rewritten = SpatialCoveringRewrite.expand(gfp, schema, geoWithCovering);

        assertThat(rewritten)
                .as("a GeometryFilterPredicate with no pruning predicate must be returned unchanged")
                .isSameAs(gfp);
    }

    @Test
    void geometryFilterWithLoweringButNoCoveringIsReturnedUnchanged() {
        ColumnPath geomCol = ColumnPath.of("geometry");
        Bbox queryBox = Bbox.of2d(0, 0, 10, 10);
        Predicate.Spatial lowering = new Predicate.Spatial.BboxIntersects(geomCol, queryBox);
        Predicate.GeometryFilterPredicate gfp =
                new Predicate.GeometryFilterPredicate(fakePruningFilter(geomCol, Optional.of(lowering)));
        Optional<GeoParquetMetadata> geoNoCovering = Optional.of(geoWithoutCovering());

        Predicate rewritten = SpatialCoveringRewrite.expand(gfp, schema, geoNoCovering);

        assertThat(rewritten)
                .as("a GeometryFilterPredicate with no resolvable covering must be returned unchanged")
                .isSameAs(gfp);
    }

    /** Walks a predicate tree and returns true when it contains a {@link Predicate.GeometryFilterPredicate} node. */
    private static boolean containsGeometryFilterPredicate(Predicate predicate) {
        return switch (predicate) {
            case Predicate.GeometryFilterPredicate _ -> true;
            case Predicate.And(List<Predicate> children) ->
                children.stream().anyMatch(SpatialCoveringRewriteTest::containsGeometryFilterPredicate);
            case Predicate.Or(List<Predicate> children) ->
                children.stream().anyMatch(SpatialCoveringRewriteTest::containsGeometryFilterPredicate);
            case Predicate.Not(Predicate child) -> containsGeometryFilterPredicate(child);
            default -> false;
        };
    }

    /**
     * Returns a fake {@link GeometryFilter} for the given column whose {@code pruningPredicate()} returns
     * {@code lowering}. The {@code decode} and {@code matches} methods throw if called, because pruning must not invoke
     * the exact gate.
     */
    private static GeometryFilter<Object> fakePruningFilter(ColumnPath geomCol, Optional<Predicate.Spatial> lowering) {
        return new GeometryFilter<>() {
            public ColumnPath column() {
                return geomCol;
            }

            public Optional<Predicate.Spatial> pruningPredicate() {
                return lowering;
            }

            public Object decode(MemorySegment wkb) {
                throw new UnsupportedOperationException("decode must not be called during pruning");
            }

            public boolean matches(Object geometry) {
                throw new UnsupportedOperationException("matches must not be called during pruning");
            }
        };
    }

    // --- fixtures ---

    /** The covering shares at least one point with the query box {@code (0, 0, 10, 10)}. */
    private static Predicate coveringOverlapsQuery() {
        return new Predicate.And(List.of(
                Pred.col(XMIN).ltEq(10.0),
                Pred.col(XMAX).gtEq(0.0),
                Pred.col(YMIN).ltEq(10.0),
                Pred.col(YMAX).gtEq(0.0)));
    }

    /** The covering holds every point of the query box {@code (0, 0, 10, 10)}. */
    private static Predicate coveringEnclosesQuery() {
        return new Predicate.And(List.of(
                Pred.col(XMIN).ltEq(0.0),
                Pred.col(XMAX).gtEq(10.0),
                Pred.col(YMIN).ltEq(0.0),
                Pred.col(YMAX).gtEq(10.0)));
    }

    private static GeoParquetMetadata geoWithCovering() {
        BboxCovering bboxCovering = new BboxCovering(XMIN, XMAX, YMIN, YMAX, Optional.empty(), Optional.empty());
        GeoColumn geometry = geoColumn(Optional.of(new Covering(bboxCovering)));
        return new GeoParquetMetadata.V1_1("1.1.0", "geometry", Map.of("geometry", geometry));
    }

    private static GeoParquetMetadata geoWithoutCovering() {
        GeoColumn geometry = geoColumn(Optional.empty());
        return new GeoParquetMetadata.V1_1("1.1.0", "geometry", Map.of("geometry", geometry));
    }

    private static GeoColumn geoColumn(Optional<Covering> covering) {
        return new GeoColumn(
                Optional.of("WKB"),
                List.of(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                OptionalDouble.empty(),
                covering);
    }

    private static ParquetSchema flatSchema(SchemaNode.Primitive... leaves) {
        List<SchemaNode> children = Stream.of(leaves).map(f -> (SchemaNode) f).toList();
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, children, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    /** A schema with a {@code geometry} leaf and a conventional {@code bbox} struct of four double leaves. */
    private static ParquetSchema schemaWithBboxStruct() {
        SchemaNode.Group bbox = new SchemaNode.Group(
                "bbox",
                Repetition.OPTIONAL,
                List.of(requiredDouble("xmin"), requiredDouble("xmax"), requiredDouble("ymin"), requiredDouble("ymax")),
                Optional.empty(),
                -1);
        SchemaNode.Group root = new SchemaNode.Group(
                "schema", Repetition.REQUIRED, List.of(requiredBinary("geometry"), bbox), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive requiredBinary(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive requiredDouble(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.DOUBLE, OptionalInt.empty(), Optional.empty(), -1);
    }
}
