/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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

import static io.tileverse.parquetry.format.ParquetLayouts.DOUBLE;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.CompressionCodec;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.GeospatialStatistics;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.format.Statistics;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.geo.geoparquet.BboxCovering;
import io.tileverse.parquetry.schema.geo.geoparquet.Covering;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoColumn;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

/**
 * Exercises the {@link SpatialBoundsSource} dispatch table plus the happy-path read paths of each backing tier. Each
 * test constructs the minimum {@link FileMetaData} / {@link ParquetSchema} / {@link GeoParquetMetadata} needed to reach
 * exactly one tier; cross-tier interactions ride on the {@code of(...)} priority order.
 */
class SpatialBoundsSourceTest {

    private static final ColumnPath GEOMETRY = ColumnPath.of("geometry");

    @Test
    void emptyWhenNoNativeNoCoveringNoBbox() {
        SpatialBoundsSource source = SpatialBoundsSource.of(footer(noStatsRowGroup()), schemaWithGeometry(), empty());
        assertThat(source)
                .as("no usable bounds in any tier should fall through to EmptyBoundsSource")
                .isInstanceOf(EmptyBoundsSource.class);
        assertThat(source.fileBounds(GEOMETRY)).isEmpty();
        assertThat(source.rowGroupBounds(GEOMETRY, 0)).isEmpty();
    }

    @Test
    void nativeStatsWinAndUnionAcrossRowGroups() {
        BoundingBox rg0 = bbox(-180, -90, -100, -50);
        BoundingBox rg1 = bbox(-90, 0, -50, 0);
        FileMetaData footer = footer(geometryRowGroupWithNative(rg0), geometryRowGroupWithNative(rg1));

        SpatialBoundsSource source = SpatialBoundsSource.of(footer, schemaWithGeometry(), empty());
        assertThat(source)
                .as("native geospatial_statistics should win the dispatch")
                .isInstanceOf(NativeStatsSource.class);
        assertThat(source.rowGroupBounds(GEOMETRY, 0))
                .as("per-row-group bbox should round-trip exactly from geospatial_statistics")
                .contains(rg0);
        assertThat(source.rowGroupBounds(GEOMETRY, 1)).contains(rg1);
        assertThat(source.fileBounds(GEOMETRY))
                .as("file-level union of [-180,-90,-100,-50] and [-90,0,-50,0]")
                .contains(bbox(-180, 0, -100, 0));
    }

    @Test
    void coveringColumnsBeatGeoJsonBbox() {
        // Schema: geometry column + bbox sidecar columns xmin/xmax/ymin/ymax (4 separate top-level DOUBLE leaves).
        ParquetSchema schema = schemaWithGeometryAndCoveringDoubles();
        FileMetaData footer = footer(coveringRowGroup(0, 1, 2, 3), coveringRowGroup(10, 20, 30, 40));
        GeoParquetMetadata geo = geoWithCoveringAndFileBbox();

        SpatialBoundsSource source = SpatialBoundsSource.of(footer, schema, of(geo));
        assertThat(source)
                .as("covering should outrank file-level geo JSON bbox")
                .isInstanceOf(CoveringColumnSource.class);
        assertThat(source.rowGroupBounds(GEOMETRY, 0))
                .as("first row group: stats decode xmin=0, xmax=1, ymin=2, ymax=3")
                .contains(bbox(0, 1, 2, 3));
        assertThat(source.rowGroupBounds(GEOMETRY, 1)).contains(bbox(10, 20, 30, 40));
        assertThat(source.fileBounds(GEOMETRY))
                .as("file union: min(xmin)=0, max(xmax)=20, min(ymin)=2, max(ymax)=40")
                .contains(bbox(0, 20, 2, 40));
    }

    @Test
    void geoJsonFileBboxUsedWhenNeitherNativeNorCoveringPresent() {
        FileMetaData footer = footer(noStatsRowGroup());
        GeoParquetMetadata geo = geoWithFileBboxOnly();
        SpatialBoundsSource source = SpatialBoundsSource.of(footer, schemaWithGeometry(), of(geo));
        assertThat(source)
                .as("file-level geo JSON bbox is the last real tier")
                .isInstanceOf(GeoJsonFileBoundsSource.class);
        assertThat(source.fileBounds(GEOMETRY))
                .as("'bbox: [-10, -5, 10, 5]' should land as BoundingBox(xmin=-10, xmax=10, ymin=-5, ymax=5)")
                .contains(bbox(-10, 10, -5, 5));
        assertThat(source.rowGroupBounds(GEOMETRY, 0))
                .as("geo JSON bbox is file-level only; row-group queries always return empty")
                .isEmpty();
    }

    @Test
    void rowGroupBoundsOutOfRangeReturnsEmpty() {
        FileMetaData footer = footer(geometryRowGroupWithNative(bbox(0, 1, 0, 1)));
        SpatialBoundsSource source = SpatialBoundsSource.of(footer, schemaWithGeometry(), empty());
        assertThat(source.rowGroupBounds(GEOMETRY, -1))
                .as("negative row-group index should surface as empty, not throw")
                .isEmpty();
        assertThat(source.rowGroupBounds(GEOMETRY, 99))
                .as("out-of-range row-group index should surface as empty, not throw")
                .isEmpty();
    }

    @Test
    void emptyForUnknownGeometryColumn() {
        FileMetaData footer = footer(geometryRowGroupWithNative(bbox(0, 1, 0, 1)));
        SpatialBoundsSource source = SpatialBoundsSource.of(footer, schemaWithGeometry(), empty());
        ColumnPath unknown = ColumnPath.of("not_a_geometry_column");
        assertThat(source.fileBounds(unknown))
                .as("unknown column on a native source should surface as empty")
                .isEmpty();
        assertThat(source.rowGroupBounds(unknown, 0)).isEmpty();
    }

    @Test
    void antimeridianBboxRoundTripsUnnormalized() {
        BoundingBox wrap = bbox(170, -170, -10, 10); // xmin > xmax: wraps the antimeridian
        assertThat(wrap.wrapsAntimeridian()).isTrue();
        FileMetaData footer = footer(geometryRowGroupWithNative(wrap));
        SpatialBoundsSource source = SpatialBoundsSource.of(footer, schemaWithGeometry(), empty());
        assertThat(source.rowGroupBounds(GEOMETRY, 0))
                .as("antimeridian-wrap bbox must round-trip verbatim; SpatialBoundsSource does not normalize")
                .contains(wrap);
    }

    // --- helpers ---

    private static BoundingBox bbox(double xmin, double xmax, double ymin, double ymax) {
        return BoundingBox.builder().xmin(xmin).xmax(xmax).ymin(ymin).ymax(ymax).build();
    }

    private static FileMetaData footer(RowGroup... rowGroups) {
        return FileMetaData.builder().version(1).rowGroups(List.of(rowGroups)).build();
    }

    /** Row group with one geometry column that carries no native stats and no covering. Triggers EmptyBoundsSource. */
    private static RowGroup noStatsRowGroup() {
        return rowGroup(List.of(columnChunk(geometryMeta(empty()))));
    }

    private static RowGroup geometryRowGroupWithNative(BoundingBox bbox) {
        GeospatialStatistics nativeStats =
                GeospatialStatistics.builder().bbox(of(bbox)).build();
        return rowGroup(List.of(columnChunk(geometryMeta(of(nativeStats)))));
    }

    /**
     * Row group with one geometry column (no native stats) and four bbox sidecar DOUBLE leaves carrying min/max stats.
     * The four leaves' min/max values are wired so each row group's bbox is exactly [xmin, xmax, ymin, ymax].
     */
    private static RowGroup coveringRowGroup(double xmin, double xmax, double ymin, double ymax) {
        return rowGroup(List.of(
                columnChunk(geometryMeta(empty())),
                columnChunk(doubleStatsMeta("xmin", xmin, xmin)),
                columnChunk(doubleStatsMeta("xmax", xmax, xmax)),
                columnChunk(doubleStatsMeta("ymin", ymin, ymin)),
                columnChunk(doubleStatsMeta("ymax", ymax, ymax))));
    }

    private static RowGroup rowGroup(List<ColumnChunk> chunks) {
        return RowGroup.builder().columns(chunks).build();
    }

    private static ColumnChunk columnChunk(ColumnMetaData meta) {
        return ColumnChunk.builder().metaData(of(meta)).build();
    }

    private static ColumnMetaData geometryMeta(Optional<GeospatialStatistics> geospatial) {
        return ColumnMetaData.builder()
                .type(PhysicalType.BYTE_ARRAY)
                .encodings(List.of(Encoding.PLAIN))
                .pathInSchema(List.of("geometry"))
                .codec(CompressionCodec.UNCOMPRESSED)
                .numValues(1L)
                .totalUncompressedSize(1L)
                .totalCompressedSize(1L)
                .dataPageOffset(0L)
                .geospatialStatistics(geospatial)
                .build();
    }

    private static ColumnMetaData doubleStatsMeta(String name, double min, double max) {
        Statistics stats = Statistics.builder()
                .minValue(littleEndianDouble(min))
                .maxValue(littleEndianDouble(max))
                .build();
        return ColumnMetaData.builder()
                .type(PhysicalType.DOUBLE)
                .encodings(List.of(Encoding.PLAIN))
                .pathInSchema(List.of(name))
                .codec(CompressionCodec.UNCOMPRESSED)
                .numValues(1L)
                .totalUncompressedSize(8L)
                .totalCompressedSize(8L)
                .dataPageOffset(0L)
                .statistics(of(stats))
                .build();
    }

    private static MemorySegment littleEndianDouble(double value) {
        MemorySegment segment = MemorySegment.ofArray(new byte[8]);
        segment.set(DOUBLE, 0, value);
        return segment.asReadOnly();
    }

    private static ParquetSchema schemaWithGeometry() {
        return new ParquetSchema(
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(geometryLeaf()), empty(), -1));
    }

    private static ParquetSchema schemaWithGeometryAndCoveringDoubles() {
        return new ParquetSchema(new SchemaNode.Group(
                "root",
                Repetition.REQUIRED,
                List.of(geometryLeaf(), doubleLeaf("xmin"), doubleLeaf("xmax"), doubleLeaf("ymin"), doubleLeaf("ymax")),
                empty(),
                -1));
    }

    private static SchemaNode.Primitive geometryLeaf() {
        return new SchemaNode.Primitive(
                "geometry", Repetition.OPTIONAL, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), empty(), -1);
    }

    private static SchemaNode.Primitive doubleLeaf(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.OPTIONAL, PrimitiveKind.DOUBLE, OptionalInt.empty(), empty(), -1);
    }

    private static GeoParquetMetadata geoWithCoveringAndFileBbox() {
        // Both a covering AND a file-level bbox; covering should win.
        BboxCovering bboxCovering = new BboxCovering(
                ColumnPath.of("xmin"),
                ColumnPath.of("xmax"),
                ColumnPath.of("ymin"),
                ColumnPath.of("ymax"),
                empty(),
                empty());
        GeoColumn col = GeoColumn.builder()
                .encoding(of("WKB"))
                .geometryTypes(List.of("Polygon"))
                .bbox(of(bbox(-180, 180, -90, 90)))
                .covering(of(new Covering(bboxCovering)))
                .build();
        return new GeoParquetMetadata.V1_1("1.1.0", "geometry", Map.of("geometry", col));
    }

    private static GeoParquetMetadata geoWithFileBboxOnly() {
        GeoColumn col = GeoColumn.builder()
                .encoding(of("WKB"))
                .geometryTypes(List.of("Polygon"))
                .bbox(of(bbox(-10, 10, -5, 5)))
                .build();
        return new GeoParquetMetadata.V1_0("1.0.0", "geometry", Map.of("geometry", col));
    }
}
