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
package io.tileverse.parquetry.filter;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.CompressionCodec;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.GeospatialStatistics;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.internal.filter.SpatialBoundsEvaluator;
import io.tileverse.parquetry.internal.filter.spatial.SpatialBoundsSource;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class SpatialPredicateTest {
    private static final ColumnPath GEOM = ColumnPath.of("geometry");
    private static final Bbox Q = Bbox.of2d(0, 0, 10, 10);

    @Test
    void buildersProduceTheFourRelations() {
        assertThat(Pred.col(GEOM).bboxIntersects(Q)).isInstanceOf(Predicate.Spatial.BboxIntersects.class);
        assertThat(Pred.col(GEOM).bboxContains(Q)).isInstanceOf(Predicate.Spatial.BboxContains.class);
        assertThat(Pred.col(GEOM).bboxCoveredBy(Q)).isInstanceOf(Predicate.Spatial.BboxCoveredBy.class);
        assertThat(Pred.col(GEOM).bboxEquals(Q)).isInstanceOf(Predicate.Spatial.BboxEquals.class);
    }

    @Test
    void spatialExposesColumnAndQueryBbox() {
        Predicate.Spatial s = (Predicate.Spatial) Pred.col(GEOM).bboxContains(Q);
        assertThat(s.col()).isEqualTo(GEOM);
        assertThat(s.bbox()).isEqualTo(Q);
    }

    @Test
    void intersectsBuilderMatchesLegacyAlias() {
        assertThat(Pred.col(GEOM).intersects(Q)).isInstanceOf(Predicate.Spatial.BboxIntersects.class);
    }

    // --- SpatialBoundsEvaluator pruning against bbox predicates ---
    // The OR-of-bboxes the renderer emits for cross-dateline and complex-projection queries must
    // eliminate a row group only when the row group's geometry bbox is disjoint from BOTH boxes,
    // keeping the region between the boxes from being scanned while never over-pruning the boxes.

    @Test
    void orEliminatesRowGroupInTheGapBetweenTwoBoxes() {
        SpatialBoundsSource bounds = boundsForGeometryBbox(bbox(40, 50, 0, 10));
        Predicate predicate = Pred.or(
                new Predicate.Spatial.BboxIntersects(GEOM, Bbox.of2d(0, 0, 10, 10)),
                new Predicate.Spatial.BboxIntersects(GEOM, Bbox.of2d(100, 0, 110, 10)));
        assertThat(SpatialBoundsEvaluator.evaluate(predicate, bounds, 0))
                .isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void orKeepsRowGroupThatIntersectsOneBox() {
        SpatialBoundsSource bounds = boundsForGeometryBbox(bbox(5, 15, 5, 15));
        Predicate predicate = Pred.or(
                new Predicate.Spatial.BboxIntersects(GEOM, Bbox.of2d(0, 0, 10, 10)),
                new Predicate.Spatial.BboxIntersects(GEOM, Bbox.of2d(100, 0, 110, 10)));
        assertThat(SpatialBoundsEvaluator.evaluate(predicate, bounds, 0))
                .isInstanceOf(PruningDecision.NotApplied.class);
    }

    @Test
    void singleBboxEliminatesDisjointRowGroup() {
        SpatialBoundsSource bounds = boundsForGeometryBbox(bbox(40, 50, 0, 10));
        Predicate predicate = new Predicate.Spatial.BboxIntersects(GEOM, Bbox.of2d(0, 0, 10, 10));
        assertThat(SpatialBoundsEvaluator.evaluate(predicate, bounds, 0))
                .isInstanceOf(PruningDecision.Eliminated.class);
    }

    // --- fixture: one row group whose geometry column has the given native bbox ---

    private static SpatialBoundsSource boundsForGeometryBbox(BoundingBox geometryBbox) {
        FileMetaData footer = footer(geometryRowGroupWithNative(geometryBbox));
        return SpatialBoundsSource.of(footer, schemaWithGeometry(), empty());
    }

    private static BoundingBox bbox(double xmin, double xmax, double ymin, double ymax) {
        return BoundingBox.builder().xmin(xmin).xmax(xmax).ymin(ymin).ymax(ymax).build();
    }

    private static FileMetaData footer(RowGroup... rowGroups) {
        return FileMetaData.builder().version(1).rowGroups(List.of(rowGroups)).build();
    }

    private static RowGroup geometryRowGroupWithNative(BoundingBox bbox) {
        GeospatialStatistics nativeStats =
                GeospatialStatistics.builder().bbox(of(bbox)).build();
        return RowGroup.builder()
                .columns(List.of(ColumnChunk.builder()
                        .metaData(of(geometryMeta(nativeStats)))
                        .build()))
                .build();
    }

    private static ColumnMetaData geometryMeta(GeospatialStatistics geospatial) {
        return ColumnMetaData.builder()
                .type(PhysicalType.BYTE_ARRAY)
                .encodings(List.of(Encoding.PLAIN))
                .pathInSchema(List.of("geometry"))
                .codec(CompressionCodec.UNCOMPRESSED)
                .numValues(1L)
                .totalUncompressedSize(1L)
                .totalCompressedSize(1L)
                .dataPageOffset(0L)
                .geospatialStatistics(of(geospatial))
                .build();
    }

    private static ParquetSchema schemaWithGeometry() {
        SchemaNode.Primitive geometryLeaf = new SchemaNode.Primitive(
                "geometry", Repetition.OPTIONAL, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), empty(), -1);
        return new ParquetSchema(new SchemaNode.Group("root", Repetition.REQUIRED, List.of(geometryLeaf), empty(), -1));
    }
}
