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
package io.tileverse.parquetry.dataset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoColumn;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

class GeoMetadataAggregatorTest {

    // GeoParquet bbox JSON is [xmin, ymin, xmax, ymax]. File A covers (0,0)-(10,10); File B covers (5,5)-(20,20).
    private static final String GEO_A =
            "{\"version\":\"1.1.0\",\"primary_column\":\"geometry\",\"columns\":{\"geometry\":{\"encoding\":\"WKB\",\"geometry_types\":[\"Point\"],\"bbox\":[0,0,10,10]}}}";
    private static final String GEO_B =
            "{\"version\":\"1.1.0\",\"primary_column\":\"geometry\",\"columns\":{\"geometry\":{\"encoding\":\"WKB\",\"geometry_types\":[\"LineString\"],\"bbox\":[5,5,20,20]}}}";
    private static final String GEO_OTHER_PRIMARY =
            "{\"version\":\"1.1.0\",\"primary_column\":\"geom\",\"columns\":{\"geom\":{\"encoding\":\"WKB\",\"geometry_types\":[\"Point\"],\"bbox\":[0,0,1,1]}}}";
    private static final String GEO_V2 =
            "{\"version\":\"2.0.0\",\"primary_column\":\"geometry\",\"columns\":{\"geometry\":{\"encoding\":\"WKB\",\"geometry_types\":[\"Point\"],\"bbox\":[0,0,10,10]}}}";

    // Same primary column and geometry as GEO_A, but with an explicit CRS where GEO_A has none.
    private static final String GEO_WITH_CRS =
            "{\"version\":\"1.1.0\",\"primary_column\":\"geometry\",\"columns\":{\"geometry\":{\"encoding\":\"WKB\",\"crs\":{\"type\":\"GeographicCRS\",\"name\":\"WGS 84\"},\"geometry_types\":[\"Point\"],\"bbox\":[0,0,10,10]}}}";

    // Same primary column and geometry as GEO_A, but encoded as native "point" rather than "WKB".
    private static final String GEO_POINT_ENCODING =
            "{\"version\":\"1.1.0\",\"primary_column\":\"geometry\",\"columns\":{\"geometry\":{\"encoding\":\"point\",\"geometry_types\":[\"Point\"],\"bbox\":[0,0,10,10]}}}";

    // Only the primary "geometry" column.
    private static final String GEO_ONLY_GEOMETRY =
            "{\"version\":\"1.1.0\",\"primary_column\":\"geometry\",\"columns\":{\"geometry\":{\"encoding\":\"WKB\",\"geometry_types\":[\"Point\"],\"bbox\":[0,0,10,10]}}}";

    // Adds a second column "geometry2" alongside the primary one.
    private static final String GEO_TWO_COLUMNS =
            "{\"version\":\"1.1.0\",\"primary_column\":\"geometry\",\"columns\":{\"geometry\":{\"encoding\":\"WKB\",\"geometry_types\":[\"Point\"],\"bbox\":[0,0,10,10]},\"geometry2\":{\"encoding\":\"WKB\",\"geometry_types\":[\"LineString\"],\"bbox\":[1,1,2,2]}}}";

    // bbox JSON [xmin, ymin, xmax, ymax] = [170, -5, -170, 5]: xmin > xmax marks an antimeridian-wrapping box.
    private static final String GEO_WRAPPING =
            "{\"version\":\"1.1.0\",\"primary_column\":\"geometry\",\"columns\":{\"geometry\":{\"encoding\":\"WKB\",\"geometry_types\":[\"Point\"],\"bbox\":[170,-5,-170,5]}}}";

    // A normal, non-wrapping box near the equator.
    private static final String GEO_NORMAL =
            "{\"version\":\"1.1.0\",\"primary_column\":\"geometry\",\"columns\":{\"geometry\":{\"encoding\":\"WKB\",\"geometry_types\":[\"Point\"],\"bbox\":[0,-2,10,8]}}}";

    // A bbox with an xmin overflowing a double: it parses as positive infinity.
    private static final String GEO_WITH_AN_INFINITE_BOUND =
            "{\"version\":\"1.1.0\",\"primary_column\":\"geometry\",\"columns\":{\"geometry\":{\"encoding\":\"WKB\",\"geometry_types\":[\"Point\"],\"bbox\":[1e999,0,110,10]}}}";

    // The primary "geometry" column without a bbox.
    private static final String GEO_WITHOUT_BBOX =
            "{\"version\":\"1.1.0\",\"primary_column\":\"geometry\",\"columns\":{\"geometry\":{\"encoding\":\"WKB\",\"geometry_types\":[\"Point\"]}}}";

    @Test
    void unionsBboxAndGeometryTypes() {
        GeoParquetMetadata a = GeoParquetMetadata.parse(GEO_A);
        GeoParquetMetadata b = GeoParquetMetadata.parse(GEO_B);
        Optional<GeoParquetMetadata> merged = GeoMetadataAggregator.aggregate(eachFile(a, b));
        assertThat(merged).isPresent();
        BoundingBox bbox = merged.get().columns().get("geometry").bbox().orElseThrow();
        assertThat(bbox.xmin()).isZero();
        assertThat(bbox.ymin()).isZero();
        assertThat(bbox.xmax()).isEqualTo(20);
        assertThat(bbox.ymax()).isEqualTo(20);
        assertThat(merged.get().columns().get("geometry").geometryTypes())
                .containsExactlyInAnyOrder("Point", "LineString");
    }

    @Test
    void preservesV2VersionSubtype() {
        GeoParquetMetadata v2 = GeoParquetMetadata.parse(GEO_V2);
        Optional<GeoParquetMetadata> merged = GeoMetadataAggregator.aggregate(eachFile(v2));
        assertThat(merged).isPresent();
        assertThat(merged.get()).isInstanceOf(GeoParquetMetadata.V2.class);
        assertThat(merged.get().version()).isEqualTo("2.0.0");
    }

    @Test
    void emptyInputIsEmpty() {
        assertThat(GeoMetadataAggregator.aggregate(List.of())).isEmpty();
    }

    @Test
    void filesWithoutGeoMetadataAloneAggregateToNothing() {
        assertThat(GeoMetadataAggregator.aggregate(List.of(Optional.empty(), Optional.empty())))
                .isEmpty();
    }

    @Test
    void aFileWithoutGeoMetadataLeavesTheBboxUnknown() {
        GeoParquetMetadata a = GeoParquetMetadata.parse(GEO_A);

        Optional<GeoParquetMetadata> merged =
                GeoMetadataAggregator.aggregate(List.of(Optional.of(a), Optional.empty()));

        GeoColumn geometry = merged.orElseThrow().columns().get("geometry");
        assertThat(geometry.bbox()).isEmpty();
        assertThat(geometry.geometryTypes()).containsExactly("Point");
    }

    @Test
    void aFileWithoutTheColumnLeavesItsBboxUnknown() {
        GeoParquetMetadata onlyGeometry = GeoParquetMetadata.parse(GEO_ONLY_GEOMETRY);
        GeoParquetMetadata twoColumns = GeoParquetMetadata.parse(GEO_TWO_COLUMNS);

        Optional<GeoParquetMetadata> merged = GeoMetadataAggregator.aggregate(eachFile(twoColumns, onlyGeometry));

        assertThat(merged.orElseThrow().columns().get("geometry").bbox()).isPresent();
        assertThat(merged.orElseThrow().columns().get("geometry2").bbox()).isEmpty();
    }

    /** A declared bbox with an infinite bound proves nothing about its file, like a missing one. */
    @Test
    void aFileDeclaringABboxWithAnInfiniteBoundLeavesTheBboxUnknown() {
        GeoParquetMetadata a = GeoParquetMetadata.parse(GEO_A);
        GeoParquetMetadata infiniteBound = GeoParquetMetadata.parse(GEO_WITH_AN_INFINITE_BOUND);

        Optional<GeoParquetMetadata> merged = GeoMetadataAggregator.aggregate(eachFile(a, infiniteBound));

        assertThat(merged.orElseThrow().columns().get("geometry").bbox()).isEmpty();
    }

    @Test
    void aFileWithoutABboxLeavesTheBboxUnknown() {
        GeoParquetMetadata a = GeoParquetMetadata.parse(GEO_A);
        GeoParquetMetadata withoutBbox = GeoParquetMetadata.parse(GEO_WITHOUT_BBOX);

        Optional<GeoParquetMetadata> merged = GeoMetadataAggregator.aggregate(eachFile(a, withoutBbox));

        assertThat(merged.orElseThrow().columns().get("geometry").bbox()).isEmpty();
    }

    @Test
    void rejectsDisagreeingPrimaryColumn() {
        GeoParquetMetadata a = GeoParquetMetadata.parse(GEO_A);
        GeoParquetMetadata other = GeoParquetMetadata.parse(GEO_OTHER_PRIMARY);
        List<Optional<GeoParquetMetadata>> disagreeing = eachFile(a, other);
        assertThatThrownBy(() -> GeoMetadataAggregator.aggregate(disagreeing))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("primary column");
    }

    @Test
    void rejectsDisagreeingCrs() {
        GeoParquetMetadata noCrs = GeoParquetMetadata.parse(GEO_A);
        GeoParquetMetadata withCrs = GeoParquetMetadata.parse(GEO_WITH_CRS);
        List<Optional<GeoParquetMetadata>> disagreeing = eachFile(noCrs, withCrs);
        assertThatThrownBy(() -> GeoMetadataAggregator.aggregate(disagreeing))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("crs");
    }

    @Test
    void unionsColumnSetAcrossFiles() {
        GeoParquetMetadata onlyGeometry = GeoParquetMetadata.parse(GEO_ONLY_GEOMETRY);
        GeoParquetMetadata twoColumns = GeoParquetMetadata.parse(GEO_TWO_COLUMNS);
        Optional<GeoParquetMetadata> merged = GeoMetadataAggregator.aggregate(eachFile(onlyGeometry, twoColumns));
        assertThat(merged).isPresent();
        assertThat(merged.get().columns()).containsKeys("geometry", "geometry2");
    }

    @Test
    void wrappingBoxYieldsFullLongitudeSuperset() {
        GeoParquetMetadata wrapping = GeoParquetMetadata.parse(GEO_WRAPPING);
        GeoParquetMetadata normal = GeoParquetMetadata.parse(GEO_NORMAL);
        Optional<GeoParquetMetadata> merged = GeoMetadataAggregator.aggregate(eachFile(wrapping, normal));
        assertThat(merged).isPresent();
        BoundingBox bbox = merged.get().columns().get("geometry").bbox().orElseThrow();
        assertThat(bbox.xmin()).isEqualTo(-180);
        assertThat(bbox.xmax()).isEqualTo(180);
        assertThat(bbox.ymin()).isEqualTo(-5);
        assertThat(bbox.ymax()).isEqualTo(8);
    }

    @Test
    void rejectsDisagreeingEncoding() {
        GeoParquetMetadata wkb = GeoParquetMetadata.parse(GEO_A);
        GeoParquetMetadata point = GeoParquetMetadata.parse(GEO_POINT_ENCODING);
        List<Optional<GeoParquetMetadata>> disagreeing = eachFile(wkb, point);
        assertThatThrownBy(() -> GeoMetadataAggregator.aggregate(disagreeing))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("encoding");
    }

    /** One entry per file, each file with geo metadata. */
    private static List<Optional<GeoParquetMetadata>> eachFile(GeoParquetMetadata... files) {
        List<Optional<GeoParquetMetadata>> perFile = new ArrayList<>(files.length);
        for (GeoParquetMetadata file : files) {
            perFile.add(Optional.of(file));
        }
        return perFile;
    }
}
