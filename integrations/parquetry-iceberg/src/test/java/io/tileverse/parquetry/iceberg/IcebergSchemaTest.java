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
package io.tileverse.parquetry.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.EdgeInterpolationAlgorithm;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.LogicalType.TimeUnit;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.geo.ParquetCrs;

class IcebergSchemaTest {

    @Test
    void presentsLeavesInFieldOrderWithIdsAndNames() {
        IcebergSchema schema = IcebergSchema.of(List.of(
                new IcebergField(5, "id", "long", true),
                new IcebergField(6, "name", "string", false),
                new IcebergField(7, "geom", "geometry", false)));

        List<SchemaNode> children = schema.parquetSchema().root().children();
        assertThat(children).hasSize(3);
        assertThat(children).allMatch(child -> child instanceof SchemaNode.Primitive);

        SchemaNode.Primitive id = (SchemaNode.Primitive) children.get(0);
        assertThat(id.name()).isEqualTo("id");
        assertThat(id.fieldId()).isEqualTo(5);
        assertThat(id.kind()).isEqualTo(PrimitiveKind.INT64);
        assertThat(id.repetition()).isEqualTo(Repetition.REQUIRED);
        assertThat(id.logicalType()).isEmpty();

        SchemaNode.Primitive name = (SchemaNode.Primitive) children.get(1);
        assertThat(name.name()).isEqualTo("name");
        assertThat(name.fieldId()).isEqualTo(6);
        assertThat(name.kind()).isEqualTo(PrimitiveKind.BYTE_ARRAY);
        assertThat(name.repetition()).isEqualTo(Repetition.OPTIONAL);
        assertThat(name.logicalType()).contains(new LogicalType.StringType());

        SchemaNode.Primitive geom = (SchemaNode.Primitive) children.get(2);
        assertThat(geom.fieldId()).isEqualTo(7);
        assertThat(geom.kind()).isEqualTo(PrimitiveKind.BYTE_ARRAY);
        assertThat(geom.logicalType()).contains(new LogicalType.Geometry(Optional.empty()));
    }

    @Test
    void mapsEveryCorpusType() {
        IcebergSchema schema = IcebergSchema.of(List.of(
                new IcebergField(1, "a", "int", false),
                new IcebergField(2, "b", "long", false),
                new IcebergField(3, "c", "float", false),
                new IcebergField(4, "d", "double", false),
                new IcebergField(5, "e", "boolean", false),
                new IcebergField(6, "f", "date", false),
                new IcebergField(7, "g", "string", false),
                new IcebergField(8, "h", "binary", false),
                new IcebergField(9, "i", "geometry", false),
                new IcebergField(10, "j", "geography", false)));

        List<SchemaNode> children = schema.parquetSchema().root().children();
        assertThat(kindOf(children, 0)).isEqualTo(PrimitiveKind.INT32);
        assertThat(kindOf(children, 1)).isEqualTo(PrimitiveKind.INT64);
        assertThat(kindOf(children, 2)).isEqualTo(PrimitiveKind.FLOAT);
        assertThat(kindOf(children, 3)).isEqualTo(PrimitiveKind.DOUBLE);
        assertThat(kindOf(children, 4)).isEqualTo(PrimitiveKind.BOOLEAN);

        assertThat(kindOf(children, 5)).isEqualTo(PrimitiveKind.INT32);
        assertThat(logicalOf(children, 5)).contains(new LogicalType.DateType());

        assertThat(kindOf(children, 6)).isEqualTo(PrimitiveKind.BYTE_ARRAY);
        assertThat(logicalOf(children, 6)).contains(new LogicalType.StringType());

        assertThat(kindOf(children, 7)).isEqualTo(PrimitiveKind.BYTE_ARRAY);
        assertThat(logicalOf(children, 7)).isEmpty();

        assertThat(logicalOf(children, 8)).contains(new LogicalType.Geometry(Optional.empty()));
        assertThat(logicalOf(children, 9)).contains(new LogicalType.Geography(Optional.empty(), Optional.empty()));
    }

    @Test
    void mapsUuidToFixedLenByteArray16WithUuidLogicalType() {
        IcebergSchema schema = IcebergSchema.of(List.of(new IcebergField(1, "u", "uuid", false)));
        SchemaNode.Primitive leaf =
                (SchemaNode.Primitive) schema.parquetSchema().root().children().get(0);
        assertThat(leaf.kind()).isEqualTo(PrimitiveKind.FIXED_LEN_BYTE_ARRAY);
        assertThat(leaf.typeLength()).hasValue(16);
        assertThat(leaf.logicalType()).contains(new LogicalType.UuidType());
    }

    @Test
    void mapsTheFourTimestampsToInt64WithTheRightAdjustmentAndUnit() {
        IcebergSchema schema = IcebergSchema.of(List.of(
                new IcebergField(1, "ts", "timestamp", false),
                new IcebergField(2, "tstz", "timestamptz", false),
                new IcebergField(3, "ts_ns", "timestamp_ns", false),
                new IcebergField(4, "tstz_ns", "timestamptz_ns", false)));
        List<SchemaNode> children = schema.parquetSchema().root().children();
        assertThat(kindOf(children, 0)).isEqualTo(PrimitiveKind.INT64);
        assertThat(logicalOf(children, 0)).contains(new LogicalType.Timestamp(false, TimeUnit.MICROS));
        assertThat(logicalOf(children, 1)).contains(new LogicalType.Timestamp(true, TimeUnit.MICROS));
        assertThat(logicalOf(children, 2)).contains(new LogicalType.Timestamp(false, TimeUnit.NANOS));
        assertThat(logicalOf(children, 3)).contains(new LogicalType.Timestamp(true, TimeUnit.NANOS));
    }

    @Test
    void mapsUnknownToInt32WithUnknownLogicalType() {
        IcebergSchema schema = IcebergSchema.of(List.of(new IcebergField(1, "x", "unknown", false)));
        SchemaNode.Primitive leaf =
                (SchemaNode.Primitive) schema.parquetSchema().root().children().get(0);
        assertThat(leaf.kind()).isEqualTo(PrimitiveKind.INT32);
        assertThat(leaf.logicalType()).contains(new LogicalType.UnknownType());
    }

    @Test
    void presentsGeometryAndGeographyCrsFromFieldComponents() {
        Optional<ParquetCrs> crs84 = ParquetCrs.reference("OGC:CRS84");
        assertThat(crs84).isPresent();
        IcebergSchema schema = IcebergSchema.of(List.of(
                new IcebergField(1, "g", "geometry", false, Optional.empty(), crs84, Optional.empty()),
                new IcebergField(
                        2,
                        "h",
                        "geography",
                        false,
                        Optional.empty(),
                        crs84,
                        Optional.of(EdgeInterpolationAlgorithm.KARNEY))));

        List<SchemaNode> children = schema.parquetSchema().root().children();
        assertThat(logicalOf(children, 0)).contains(new LogicalType.Geometry(crs84));
        assertThat(logicalOf(children, 1))
                .contains(new LogicalType.Geography(crs84, Optional.of(EdgeInterpolationAlgorithm.KARNEY)));
    }

    @Test
    void rejectsUnmappedType() {
        List<IcebergField> unmapped = List.of(new IcebergField(1, "amount", "decimal", false));
        assertThatThrownBy(() -> IcebergSchema.of(unmapped))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("decimal");
    }

    @Test
    void looksUpFieldById() {
        IcebergSchema schema = IcebergSchema.of(
                List.of(new IcebergField(3, "id", "long", false), new IcebergField(8, "name", "string", false)));

        assertThat(schema.fieldById(8)).contains(new IcebergField(8, "name", "string", false));
        assertThat(schema.fieldById(99)).isEmpty();
    }

    @Test
    void rootGroupHasNoLeafPathName() {
        IcebergSchema schema = IcebergSchema.of(List.of(new IcebergField(1, "id", "long", false)));
        ParquetSchema presented = schema.parquetSchema();
        assertThat(presented.leafColumns())
                .singleElement()
                .extracting(path -> path.dot())
                .isEqualTo("id");
    }

    private static PrimitiveKind kindOf(List<SchemaNode> children, int index) {
        return ((SchemaNode.Primitive) children.get(index)).kind();
    }

    private static Optional<LogicalType> logicalOf(List<SchemaNode> children, int index) {
        return ((SchemaNode.Primitive) children.get(index)).logicalType();
    }
}
