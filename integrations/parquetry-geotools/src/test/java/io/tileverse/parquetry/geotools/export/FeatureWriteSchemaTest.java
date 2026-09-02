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
package io.tileverse.parquetry.geotools.export;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.stream.Stream;

import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.data.nested.NestedType;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.CRS;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.locationtech.jts.geom.Point;

import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.LogicalType.TimeUnit;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoColumn;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

class FeatureWriteSchemaTest {

    @ParameterizedTest
    @MethodSource("scalarBindings")
    void scalarBindingProducesExpectedLeaf(Class<?> binding, PrimitiveKind kind, Optional<LogicalType> logical) {
        SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
        typeBuilder.setName("t");
        typeBuilder.add("attr", binding);
        SimpleFeatureType type = typeBuilder.buildFeatureType();

        FeatureWriteSchema writeSchema = FeatureWriteSchema.of(type);

        SchemaNode.Primitive leaf = leafFor(writeSchema, "attr");
        assertThat(leaf.repetition()).isEqualTo(Repetition.OPTIONAL);
        assertThat(leaf.kind()).isEqualTo(kind);
        assertThat(leaf.logicalType()).isEqualTo(logical);
        OptionalInt expectedTypeLength = binding == UUID.class ? OptionalInt.of(16) : OptionalInt.empty();
        assertThat(leaf.typeLength()).isEqualTo(expectedTypeLength);
    }

    private static Stream<Arguments> scalarBindings() {
        return Stream.of(
                Arguments.of(String.class, PrimitiveKind.BYTE_ARRAY, Optional.of(new LogicalType.StringType())),
                Arguments.of(Integer.class, PrimitiveKind.INT32, Optional.empty()),
                Arguments.of(Short.class, PrimitiveKind.INT32, Optional.empty()),
                Arguments.of(Byte.class, PrimitiveKind.INT32, Optional.empty()),
                Arguments.of(Long.class, PrimitiveKind.INT64, Optional.empty()),
                Arguments.of(BigInteger.class, PrimitiveKind.INT64, Optional.empty()),
                Arguments.of(Float.class, PrimitiveKind.FLOAT, Optional.empty()),
                Arguments.of(Double.class, PrimitiveKind.DOUBLE, Optional.empty()),
                Arguments.of(BigDecimal.class, PrimitiveKind.DOUBLE, Optional.empty()),
                Arguments.of(Boolean.class, PrimitiveKind.BOOLEAN, Optional.empty()),
                Arguments.of(byte[].class, PrimitiveKind.BYTE_ARRAY, Optional.empty()),
                Arguments.of(UUID.class, PrimitiveKind.FIXED_LEN_BYTE_ARRAY, Optional.of(new LogicalType.UuidType())),
                Arguments.of(LocalDate.class, PrimitiveKind.INT32, Optional.of(new LogicalType.DateType())),
                Arguments.of(java.sql.Date.class, PrimitiveKind.INT32, Optional.of(new LogicalType.DateType())),
                Arguments.of(
                        Instant.class,
                        PrimitiveKind.INT64,
                        Optional.of(new LogicalType.Timestamp(true, TimeUnit.MICROS))),
                Arguments.of(
                        java.util.Date.class,
                        PrimitiveKind.INT64,
                        Optional.of(new LogicalType.Timestamp(true, TimeUnit.MICROS))),
                Arguments.of(
                        java.sql.Timestamp.class,
                        PrimitiveKind.INT64,
                        Optional.of(new LogicalType.Timestamp(true, TimeUnit.MICROS))),
                Arguments.of(
                        LocalDateTime.class,
                        PrimitiveKind.INT64,
                        Optional.of(new LogicalType.Timestamp(false, TimeUnit.MICROS))));
    }

    @Test
    void geometryColumnRegistersEpsgAndGeoMetadata() throws Exception {
        SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
        typeBuilder.setName("t");
        typeBuilder.setCRS(CRS.decode("EPSG:4326", true));
        typeBuilder.add("geom", Point.class);
        SimpleFeatureType type = typeBuilder.buildFeatureType();

        FeatureWriteSchema writeSchema = FeatureWriteSchema.of(type);

        SchemaNode.Primitive leaf = leafFor(writeSchema, "geom");
        assertThat(leaf.repetition()).isEqualTo(Repetition.OPTIONAL);
        assertThat(leaf.kind()).isEqualTo(PrimitiveKind.BYTE_ARRAY);
        assertThat(leaf.logicalType()).isEmpty();

        assertThat(writeSchema.geometryEpsg()).containsExactly(Map.entry("geom", 4326));

        assertThat(writeSchema.geoMetadata()).isPresent();
        GeoParquetMetadata geo = writeSchema.geoMetadata().orElseThrow();
        assertThat(geo).isInstanceOf(GeoParquetMetadata.V1_1.class);
        assertThat(geo.primaryColumn()).isEqualTo("geom");
        assertThat(geo.columns()).containsOnlyKeys("geom");
        GeoColumn column = geo.columns().get("geom");
        assertThat(column.encoding()).contains("WKB");
        assertThat(column.crs()).isPresent();
    }

    @Test
    void nullCrsGeometryOmitsCrs() {
        SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
        typeBuilder.setName("t");
        typeBuilder.add("geom", Point.class);
        SimpleFeatureType type = typeBuilder.buildFeatureType();

        FeatureWriteSchema writeSchema = FeatureWriteSchema.of(type);

        SchemaNode.Primitive leaf = leafFor(writeSchema, "geom");
        assertThat(leaf.repetition()).isEqualTo(Repetition.OPTIONAL);
        assertThat(leaf.kind()).isEqualTo(PrimitiveKind.BYTE_ARRAY);
        assertThat(leaf.logicalType()).isEmpty();

        assertThat(writeSchema.geometryEpsg()).isEmpty();

        assertThat(writeSchema.geoMetadata()).isPresent();
        GeoColumn column = writeSchema.geoMetadata().orElseThrow().columns().get("geom");
        assertThat(column.crs()).isEmpty();
    }

    @Test
    void nonSpatialTypeHasNoGeoMetadata() {
        SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
        typeBuilder.setName("t");
        typeBuilder.add("name", String.class);
        SimpleFeatureType type = typeBuilder.buildFeatureType();

        FeatureWriteSchema writeSchema = FeatureWriteSchema.of(type);

        assertThat(writeSchema.geoMetadata()).isEmpty();
        assertThat(writeSchema.geometryEpsg()).isEmpty();
    }

    @Test
    void unsupportedBindingFailsWithAttributeName() {
        SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
        typeBuilder.setName("t");
        typeBuilder.add("badAttr", java.sql.Time.class);
        SimpleFeatureType type = typeBuilder.buildFeatureType();

        assertThatThrownBy(() -> FeatureWriteSchema.of(type))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("badAttr")
                .hasMessageContaining("Time");
    }

    @Test
    void listWithoutNestedTypeUserDataIsRejected() {
        SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
        typeBuilder.setName("t");
        typeBuilder.add("tags", List.class);
        SimpleFeatureType type = typeBuilder.buildFeatureType();

        assertThatThrownBy(() -> FeatureWriteSchema.of(type))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tags");
    }

    @Test
    void listOfListDerivesNestedThreeLevelGroups() {
        SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
        typeBuilder.setName("t");
        typeBuilder.userData(
                NestedType.USER_DATA_KEY,
                new NestedType.ListType(new NestedType.ListType(new NestedType.ScalarType(Integer.class))));
        typeBuilder.add("matrix", List.class);
        SimpleFeatureType type = typeBuilder.buildFeatureType();

        FeatureWriteSchema writeSchema = FeatureWriteSchema.of(type);

        SchemaNode.Primitive innerLeaf = (SchemaNode.Primitive) writeSchema
                .schema()
                .find(ColumnPath.of("matrix", "list", "element", "list", "element"))
                .orElseThrow();
        assertThat(innerLeaf.kind()).isEqualTo(PrimitiveKind.INT32);
        assertThat(innerLeaf.repetition()).isEqualTo(Repetition.OPTIONAL);
    }

    @Test
    void mapWithListValueDerivesListUnderTheEntryValue() {
        SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
        typeBuilder.setName("t");
        typeBuilder.userData(
                NestedType.USER_DATA_KEY,
                new NestedType.MapType(
                        new NestedType.ScalarType(String.class),
                        new NestedType.ListType(new NestedType.ScalarType(String.class))));
        typeBuilder.add("groups", Map.class);
        SimpleFeatureType type = typeBuilder.buildFeatureType();

        FeatureWriteSchema writeSchema = FeatureWriteSchema.of(type);

        SchemaNode.Primitive keyLeaf = (SchemaNode.Primitive) writeSchema
                .schema()
                .find(ColumnPath.of("groups", "key_value", "key"))
                .orElseThrow();
        assertThat(keyLeaf.repetition()).isEqualTo(Repetition.REQUIRED);

        SchemaNode.Primitive elementLeaf = (SchemaNode.Primitive) writeSchema
                .schema()
                .find(ColumnPath.of("groups", "key_value", "value", "list", "element"))
                .orElseThrow();
        assertThat(elementLeaf.kind()).isEqualTo(PrimitiveKind.BYTE_ARRAY);
    }

    private static SchemaNode.Primitive leafFor(FeatureWriteSchema writeSchema, String name) {
        return (SchemaNode.Primitive)
                writeSchema.schema().find(ColumnPath.of(name)).orElseThrow();
    }
}
