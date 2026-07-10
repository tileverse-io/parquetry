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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;

import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.feature.type.AttributeDescriptor;
import org.geotools.api.feature.type.GeometryDescriptor;
import org.geotools.data.nested.NestedType;
import org.geotools.data.nested.NestedType.ListType;
import org.geotools.data.nested.NestedType.MapType;
import org.geotools.data.nested.NestedType.StructType;
import org.geotools.referencing.CRS;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;

import io.tileverse.parquetry.catalog.FilesetCatalog;
import io.tileverse.parquetry.dataset.GeoParquetDataset;
import io.tileverse.parquetry.dataset.ParquetSource;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.LogicalType.TimeUnit;
import io.tileverse.parquetry.geotools.data.FeatureTypeMapper.AttributeMapping;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.geo.ParquetCrs;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;
import io.tileverse.parquetry.testkit.TestCorpus;

class FeatureTypeMapperTest {

    private static List<AttributeMapping> attributes() {
        return List.of(
                new AttributeMapping("geom", ColumnPath.of("geometry"), true, Point.class),
                new AttributeMapping("id", ColumnPath.of("id"), false, Long.class),
                new AttributeMapping("name", ColumnPath.of("name"), false, String.class),
                new AttributeMapping("blob", ColumnPath.of("blob"), false, byte[].class));
    }

    @Test
    void autoDetectsIdColumnAsFeatureId() {
        Optional<AttributeMapping> fid = FeatureTypeMapper.resolveFidAttribute("t", null, attributes());
        assertThat(fid).map(AttributeMapping::name).hasValue("id");
    }

    @Test
    void configuredColumnOverridesAutoDetection() {
        Optional<AttributeMapping> fid = FeatureTypeMapper.resolveFidAttribute("t", "name", attributes());
        assertThat(fid).map(AttributeMapping::name).hasValue("name");
    }

    @Test
    void noIdColumnAndNoOverrideResolvesToNone() {
        List<AttributeMapping> noId = List.of(
                new AttributeMapping("geom", ColumnPath.of("geometry"), true, Point.class),
                new AttributeMapping("name", ColumnPath.of("name"), false, String.class));
        assertThat(FeatureTypeMapper.resolveFidAttribute("t", null, noId)).isEmpty();
    }

    @Test
    void configuredMissingColumnIsRejected() {
        List<AttributeMapping> attributes = attributes();
        assertThatThrownBy(() -> FeatureTypeMapper.resolveFidAttribute("t", "nope", attributes))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nope");
    }

    @Test
    void configuredGeometryColumnIsRejected() {
        List<AttributeMapping> attributes = attributes();
        assertThatThrownBy(() -> FeatureTypeMapper.resolveFidAttribute("t", "geom", attributes))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void configuredBinaryColumnIsRejected() {
        List<AttributeMapping> attributes = attributes();
        assertThatThrownBy(() -> FeatureTypeMapper.resolveFidAttribute("t", "blob", attributes))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void bindsUuidLogicalTypeToJavaUuid() {
        Optional<Class<?>> binding =
                FeatureTypeMapper.resolveBinding(PrimitiveKind.FIXED_LEN_BYTE_ARRAY, uuidLogicalType());
        assertThat(binding).hasValue(UUID.class);
    }

    @Test
    void bindsPlainFixedLenByteArrayToByteArray() {
        Optional<Class<?>> binding =
                FeatureTypeMapper.resolveBinding(PrimitiveKind.FIXED_LEN_BYTE_ARRAY, Optional.empty());
        assertThat(binding).hasValue(byte[].class);
    }

    @Test
    void bindsDateToLocalDate() {
        Optional<Class<?>> binding =
                FeatureTypeMapper.resolveBinding(PrimitiveKind.INT32, Optional.of(new LogicalType.DateType()));
        assertThat(binding).hasValue(LocalDate.class);
    }

    @Test
    void bindsUtcTimestampToInstant() {
        Optional<Class<?>> binding = FeatureTypeMapper.resolveBinding(
                PrimitiveKind.INT64, Optional.of(new LogicalType.Timestamp(true, TimeUnit.MICROS)));
        assertThat(binding).hasValue(Instant.class);
    }

    @Test
    void bindsLocalTimestampToLocalDateTime() {
        Optional<Class<?>> binding = FeatureTypeMapper.resolveBinding(
                PrimitiveKind.INT64, Optional.of(new LogicalType.Timestamp(false, TimeUnit.NANOS)));
        assertThat(binding).hasValue(LocalDateTime.class);
    }

    @Test
    void bindsPlainInt64ToLong() {
        Optional<Class<?>> binding = FeatureTypeMapper.resolveBinding(PrimitiveKind.INT64, Optional.empty());
        assertThat(binding).hasValue(Long.class);
    }

    @Test
    void autoDetectsUuidIdColumnAsFeatureId() {
        Optional<AttributeMapping> fid = FeatureTypeMapper.resolveFidAttribute("t", null, attributesWithUuidId());
        assertThat(fid).map(AttributeMapping::name).hasValue("id");
    }

    @Test
    void configuredUuidColumnIsAcceptedAsFeatureId() {
        Optional<AttributeMapping> fid = FeatureTypeMapper.resolveFidAttribute("t", "id", attributesWithUuidId());
        assertThat(fid).map(AttributeMapping::binding).hasValue(UUID.class);
    }

    private static Optional<LogicalType> uuidLogicalType() {
        return Optional.of(new LogicalType.UuidType());
    }

    private static List<AttributeMapping> attributesWithUuidId() {
        return List.of(
                new AttributeMapping("geom", ColumnPath.of("geometry"), true, Point.class),
                new AttributeMapping("id", ColumnPath.of("id"), false, UUID.class),
                new AttributeMapping("name", ColumnPath.of("name"), false, String.class));
    }

    @Test
    void mapsExampleGeoParquetToSimpleFeatureType(@TempDir Path dir) throws Exception {
        Path file = TestCorpus.extractFile("geoparquet/examples/example.parquet", dir);
        try (ByteRangeSource src = ByteRangeSource.ofFile(file)) {
            ParquetSource ds = ParquetSource.open(src);
            Optional<GeoParquetMetadata> geo =
                    Optional.of(GeoParquetMetadata.parse(ds.keyValueMetadata().get("geo")));
            FeatureTypeMapper.Mapping mapping = FeatureTypeMapper.map("example", null, ds.schema(), geo, null);
            SimpleFeatureType ft = mapping.featureType();

            GeometryDescriptor geom = ft.getGeometryDescriptor();
            assertThat(geom).isNotNull();
            assertThat(geom.getType().getBinding()).isAssignableFrom(Geometry.class);
            assertThat(ft.getCoordinateReferenceSystem()).isNotNull();

            assertThat(ft.getAttributeDescriptors())
                    .extracting(d -> d.getLocalName())
                    .contains("name");
            assertThat(mapping.attributes()).isNotEmpty();
            assertThat(mapping.attributes())
                    .allSatisfy(a -> assertThat(a.path()).isNotNull());
        }
    }

    @Test
    void mapsNestedColumnsToSingleAttributesWithoutFlattening(@TempDir Path dir) throws Exception {
        FeatureTypeMapper.Mapping mapping = mapNestedFixture(dir);
        SimpleFeatureType ft = mapping.featureType();

        List<String> names = ft.getAttributeDescriptors().stream()
                .map(AttributeDescriptor::getLocalName)
                .toList();
        assertThat(names).containsExactlyInAnyOrder("id", "geometry", "brand", "addresses", "tags");
        assertThat(names)
                .as("nested columns must not be flattened to dotted attribute names")
                .doesNotContain("addresses.list.element.locality", "brand.name");
    }

    @Test
    void bindsNestedAttributesToListOrMap(@TempDir Path dir) throws Exception {
        SimpleFeatureType ft = mapNestedFixture(dir).featureType();

        assertThat(ft.getDescriptor("addresses").getType().getBinding()).isEqualTo(List.class);
        assertThat(ft.getDescriptor("brand").getType().getBinding()).isEqualTo(Map.class);
        assertThat(ft.getDescriptor("tags").getType().getBinding()).isEqualTo(Map.class);
    }

    @Test
    void recordsNestedTypeInDescriptorUserData(@TempDir Path dir) throws Exception {
        SimpleFeatureType ft = mapNestedFixture(dir).featureType();

        assertThat(ft.getDescriptor("addresses").getUserData().get(NestedType.USER_DATA_KEY))
                .isInstanceOf(ListType.class);
        assertThat(ft.getDescriptor("brand").getUserData().get(NestedType.USER_DATA_KEY))
                .isInstanceOf(StructType.class);
        assertThat(ft.getDescriptor("tags").getUserData().get(NestedType.USER_DATA_KEY))
                .isInstanceOf(MapType.class);
    }

    @Test
    void nestedAttributeMappingExposesItsTopLevelPathAndType(@TempDir Path dir) throws Exception {
        FeatureTypeMapper.Mapping mapping = mapNestedFixture(dir);

        AttributeMapping addresses = attribute(mapping, "addresses");
        assertThat(addresses.nestedType()).isInstanceOf(ListType.class);
        assertThat(addresses.path()).isEqualTo(ColumnPath.of("addresses"));

        AttributeMapping id = attribute(mapping, "id");
        assertThat(id.nestedType()).isNull();
    }

    @Test
    void keepsGeometryAsTheDefaultGeometryOverNestedColumns(@TempDir Path dir) throws Exception {
        SimpleFeatureType ft = mapNestedFixture(dir).featureType();

        GeometryDescriptor geom = ft.getGeometryDescriptor();
        assertThat(geom).isNotNull();
        assertThat(geom.getLocalName()).isEqualTo("geometry");
        assertThat(geom.getType().getBinding()).isAssignableFrom(Geometry.class);
    }

    private static FeatureTypeMapper.Mapping mapNestedFixture(Path dir) throws Exception {
        Path file = dir.resolve("nested.parquet");
        NestedFixtures.writeSample(file);
        try (FilesetCatalog catalog = NestedFixtures.openCatalog(file)) {
            GeoParquetDataset dataset = (GeoParquetDataset) catalog.dataset("nested");
            return FeatureTypeMapper.map("nested", null, dataset.schema(), dataset.geoMetadata(), null);
        }
    }

    private static AttributeMapping attribute(FeatureTypeMapper.Mapping mapping, String name) {
        return mapping.attributes().stream()
                .filter(attr -> attr.name().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no attribute named '" + name + "'"));
    }

    @Test
    void hidesDeclaredBboxCoveringColumnsFromTheFeatureType() {
        ParquetSchema schema = schemaWithDeclaredCovering();
        String geoJson = """
                {"version":"1.1.0","primary_column":"geom","columns":{"geom":{"encoding":"WKB",\
                "geometry_types":[],"covering":{"bbox":{"xmin":["geom_bbox","xmin"],"ymin":["geom_bbox","ymin"],\
                "xmax":["geom_bbox","xmax"],"ymax":["geom_bbox","ymax"]}}}}}""";

        FeatureTypeMapper.Mapping mapping =
                FeatureTypeMapper.map("countries", null, schema, Optional.of(GeoParquetMetadata.parse(geoJson)), null);

        List<String> attributeNames = mapping.featureType().getAttributeDescriptors().stream()
                .map(descriptor -> descriptor.getLocalName())
                .toList();
        assertThat(attributeNames).contains("geom", "name");
        assertThat(attributeNames)
                .as("the bbox covering is an internal spatial index, not a user attribute")
                .doesNotContain("geom_bbox.xmin", "geom_bbox.ymin", "geom_bbox.xmax", "geom_bbox.ymax");
    }

    @Test
    void resolvesNativeAuthorityCodeCrsToEpsg() throws Exception {
        ParquetSchema schema = schemaWithNativeGeometryCrs(new ParquetCrs.AuthorityCode("EPSG", "3857"));

        FeatureTypeMapper.Mapping mapping = FeatureTypeMapper.map("native", null, schema, Optional.empty(), null);
        org.geotools.api.referencing.crs.CoordinateReferenceSystem crs =
                mapping.featureType().getCoordinateReferenceSystem();

        assertThat(CRS.lookupEpsgCode(crs, false))
                .as("an EPSG:3857 native crs reference resolves to EPSG:3857 through the registry")
                .isEqualTo(3857);
    }

    private static ParquetSchema schemaWithNativeGeometryCrs(ParquetCrs crs) {
        SchemaNode.Primitive geom = new SchemaNode.Primitive(
                "geom",
                Repetition.OPTIONAL,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.Geometry(Optional.of(crs))),
                -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(geom), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static ParquetSchema schemaWithDeclaredCovering() {
        SchemaNode.Primitive geom = new SchemaNode.Primitive(
                "geom",
                Repetition.OPTIONAL,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.Geometry(Optional.empty())),
                -1);
        SchemaNode.Primitive name = new SchemaNode.Primitive(
                "name",
                Repetition.OPTIONAL,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.StringType()),
                -1);
        SchemaNode.Group bbox = new SchemaNode.Group(
                "geom_bbox",
                Repetition.OPTIONAL,
                List.of(coveringLeaf("xmin"), coveringLeaf("ymin"), coveringLeaf("xmax"), coveringLeaf("ymax")),
                Optional.empty(),
                -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(geom, name, bbox), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive coveringLeaf(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.OPTIONAL, PrimitiveKind.FLOAT, OptionalInt.empty(), Optional.empty(), -1);
    }
}
