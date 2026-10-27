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
package io.tileverse.parquetry.internal.write;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ParquetWriteException;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.CoveringMode;
import io.tileverse.parquetry.data.WriteOptions.GeoParquetMetadataMode;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class BboxCoveringPlanTest {

    private static ParquetSchema geoSchema() {
        SchemaNode.Primitive geometry = new SchemaNode.Primitive(
                "geometry", Repetition.OPTIONAL, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(geometry), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static WriteOptions.Builder geoOptions() {
        return WriteOptions.builder().crsEpsg("geometry", 4326);
    }

    private static BboxCoveringPlan resolve(WriteOptions options, ParquetSchema schema) {
        return BboxCoveringPlan.resolve(options, schema, new GeoMetadataWriter(options));
    }

    @Test
    void dualDefaultResolvesToFloatCoveringForWgs84() {
        BboxCoveringPlan plan = resolve(geoOptions().build(), geoSchema());
        assertThat(plan.active()).isTrue();
        assertThat(plan.writtenSchema().find(ColumnPath.of("bbox", "xmin"))).isPresent();
        SchemaNode.Primitive xmin = (SchemaNode.Primitive)
                plan.writtenSchema().find(ColumnPath.of("bbox", "xmin")).orElseThrow();
        assertThat(xmin.kind()).isEqualTo(PrimitiveKind.FLOAT);
    }

    // The default covering follows one rule in all metadata modes.
    @Test
    void v2OnlyDefaultResolvesToFloatCovering() {
        WriteOptions options = geoOptions()
                .geoParquetMetadata(GeoParquetMetadataMode.V2_0_ONLY)
                .build();
        BboxCoveringPlan plan = resolve(options, geoSchema());
        assertThat(plan.active()).isTrue();
        SchemaNode.Primitive xmin = (SchemaNode.Primitive)
                plan.writtenSchema().find(ColumnPath.of("bbox", "xmin")).orElseThrow();
        assertThat(xmin.kind()).isEqualTo(PrimitiveKind.FLOAT);
    }

    @Test
    void v2OnlyNoneResolvesToNoCovering() {
        WriteOptions options = geoOptions()
                .geoParquetMetadata(GeoParquetMetadataMode.V2_0_ONLY)
                .bboxCovering(CoveringMode.NONE)
                .build();
        BboxCoveringPlan plan = resolve(options, geoSchema());
        assertThat(plan.active()).isFalse();
        assertThat(plan.writtenSchema()).isEqualTo(geoSchema());
    }

    @Test
    void autoOnAProjectedCrsResolvesToDouble() {
        WriteOptions options = WriteOptions.builder().crsEpsg("geometry", 3857).build();
        BboxCoveringPlan plan = resolve(options, geoSchema());
        SchemaNode.Primitive xmin = (SchemaNode.Primitive)
                plan.writtenSchema().find(ColumnPath.of("bbox", "xmin")).orElseThrow();
        assertThat(xmin.kind()).isEqualTo(PrimitiveKind.DOUBLE);
    }

    @Test
    void defaultedCoveringWithNoGeometryIsInactive() {
        ParquetSchema nonGeo = geoSchema();
        BboxCoveringPlan plan = BboxCoveringPlan.resolve(
                WriteOptions.defaults(), nonGeo, new GeoMetadataWriter(WriteOptions.defaults()));
        assertThat(plan.active()).isFalse();
    }

    @Test
    void explicitCoveringWithNoGeometryThrows() {
        WriteOptions options =
                WriteOptions.builder().bboxCovering(CoveringMode.FLOAT).build();
        ParquetSchema schema = geoSchema();

        assertThatThrownBy(() -> resolve(options, schema))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("geometry");
    }

    private static ParquetSchema geoSchemaWithExtent(PrimitiveKind extentKind) {
        return geoSchemaWithExtent(extentKind, Repetition.OPTIONAL, "xmin", "ymin", "xmax", "ymax");
    }

    /** An optional geometry column and an {@code extent} struct with one leaf per name, in the given order. */
    private static ParquetSchema geoSchemaWithExtent(
            PrimitiveKind extentKind, Repetition extentRepetition, String... leafNames) {
        SchemaNode.Primitive geometry = new SchemaNode.Primitive(
                "geometry", Repetition.OPTIONAL, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        List<SchemaNode> extentLeaves = new ArrayList<>();
        for (String name : leafNames) {
            extentLeaves.add(extentLeaf(name, extentKind));
        }
        SchemaNode.Group extent = new SchemaNode.Group("extent", extentRepetition, extentLeaves, Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(geometry, extent), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive extentLeaf(String name, PrimitiveKind kind) {
        return new SchemaNode.Primitive(name, Repetition.OPTIONAL, kind, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static WriteOptions.Builder existingExtentCovering() {
        return geoOptions()
                .existingBboxCovering("geometry", "extent.xmin", "extent.ymin", "extent.xmax", "extent.ymax");
    }

    private static byte[] wkbPoint(double x, double y) {
        ByteBuffer buffer = ByteBuffer.allocate(21).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put((byte) 1);
        buffer.putInt(1);
        buffer.putDouble(x);
        buffer.putDouble(y);
        return buffer.array();
    }

    @Test
    void existingCoveringIsDeclaredOverTheSchemaAsIs() {
        ParquetSchema schema = geoSchemaWithExtent(PrimitiveKind.FLOAT);

        BboxCoveringPlan plan = resolve(existingExtentCovering().build(), schema);

        assertThat(plan.active()).isTrue();
        assertThat(plan.derivesColumns()).isFalse();
        assertThat(plan.writtenSchema()).isEqualTo(schema);
        assertThat(plan.geometryColumn()).isEqualTo(ColumnPath.of("geometry"));
        assertThat(plan.xmin()).isEqualTo(ColumnPath.of("extent", "xmin"));
        assertThat(plan.ymin()).isEqualTo(ColumnPath.of("extent", "ymin"));
        assertThat(plan.xmax()).isEqualTo(ColumnPath.of("extent", "xmax"));
        assertThat(plan.ymax()).isEqualTo(ColumnPath.of("extent", "ymax"));
    }

    @Test
    void existingCoveringAcceptsDoubleLeaves() {
        ParquetSchema schema = geoSchemaWithExtent(PrimitiveKind.DOUBLE);

        BboxCoveringPlan plan = resolve(existingExtentCovering().build(), schema);

        assertThat(plan.active()).isTrue();
        assertThat(plan.writtenSchema()).isEqualTo(schema);
    }

    @Test
    void existingCoveringLeavesBatchesUntouched() {
        ParquetSchema schema = geoSchemaWithExtent(PrimitiveKind.FLOAT);
        BboxCoveringPlan plan = resolve(existingExtentCovering().build(), schema);
        Map<ColumnPath, Object> row = Map.of(
                ColumnPath.of("geometry"), wkbPoint(1.0, 2.0),
                ColumnPath.of("extent", "xmin"), 1.0f,
                ColumnPath.of("extent", "ymin"), 2.0f,
                ColumnPath.of("extent", "xmax"), 1.0f,
                ColumnPath.of("extent", "ymax"), 2.0f);
        ParquetRecordBatch raw = WriteFixtures.batch(schema, List.of(row));

        assertThat(plan.augment(raw)).isSameAs(raw);
    }

    @Test
    void existingCoveringOverAMissingLeafThrows() {
        ParquetSchema schema = geoSchema();
        WriteOptions options = existingExtentCovering().build();

        assertThatThrownBy(() -> resolve(options, schema))
                .isInstanceOf(ParquetWriteException.class)
                .hasMessageContaining("no 'extent' column");
    }

    @Test
    void existingCoveringIsReordered() {
        ParquetSchema schema =
                geoSchemaWithExtent(PrimitiveKind.FLOAT, Repetition.OPTIONAL, "xmin", "xmax", "ymin", "ymax");

        BboxCoveringPlan plan = resolve(existingExtentCovering().build(), schema);

        assertThat(plan.active()).isTrue();
        assertThat(plan.writtenSchema()).isEqualTo(geoSchemaWithExtent(PrimitiveKind.FLOAT));
    }

    @Test
    void existingRepetitionMustMatch() {
        ParquetSchema schema =
                geoSchemaWithExtent(PrimitiveKind.FLOAT, Repetition.REQUIRED, "xmin", "ymin", "xmax", "ymax");
        WriteOptions options = existingExtentCovering().build();

        assertThatThrownBy(() -> resolve(options, schema))
                .isInstanceOf(ParquetWriteException.class)
                .hasMessageContaining("'extent' column is REQUIRED");
    }

    // GeoParquet requires the covering paths to name the fields of one top-level struct.
    @Test
    void existingOverFlatColumnsThrows() {
        ParquetSchema schema = geoSchemaWithExtent(PrimitiveKind.FLOAT);
        WriteOptions options = geoOptions()
                .existingBboxCovering("geometry", "extent.xmin", "extent.ymin", "extent.xmax", "geometry")
                .build();

        assertThatThrownBy(() -> resolve(options, schema))
                .isInstanceOf(ParquetWriteException.class)
                .hasMessageContaining("one top-level struct");
    }

    @Test
    void existingCoveringOverANonNumericLeafThrows() {
        ParquetSchema schema = geoSchemaWithExtent(PrimitiveKind.INT32);
        WriteOptions options = existingExtentCovering().build();

        assertThatThrownBy(() -> resolve(options, schema))
                .isInstanceOf(ParquetWriteException.class)
                .hasMessageContaining("FLOAT or DOUBLE");
    }

    // A GeoParquet 1.0 file may hold this struct undeclared, with its fields in another order than the spec's.
    @Test
    void autoDeclaresTheBboxStruct() {
        ParquetSchema schema = geoSchemaWithBbox("xmin", "xmax", "ymin", "ymax");

        BboxCoveringPlan plan = resolve(geoOptions().build(), schema);

        assertThat(plan.active()).isTrue();
        assertThat(plan.derivesColumns()).isFalse();
        assertThat(plan.writtenSchema())
                .as("the struct is written with its fields in the spec order")
                .isEqualTo(geoSchemaWithBbox("xmin", "ymin", "xmax", "ymax"));
        assertThat(plan.xmin()).isEqualTo(ColumnPath.of("bbox", "xmin"));
        assertThat(plan.ymin()).isEqualTo(ColumnPath.of("bbox", "ymin"));
        assertThat(plan.xmax()).isEqualTo(ColumnPath.of("bbox", "xmax"));
        assertThat(plan.ymax()).isEqualTo(ColumnPath.of("bbox", "ymax"));
    }

    @Test
    void explicitAutoDeclaresBboxStruct() {
        ParquetSchema schema = geoSchemaWithBbox("xmin", "ymin", "xmax", "ymax");
        WriteOptions options = geoOptions().bboxCovering(CoveringMode.AUTO).build();

        BboxCoveringPlan plan = resolve(options, schema);

        assertThat(plan.active()).isTrue();
        assertThat(plan.derivesColumns()).isFalse();
    }

    @Test
    void incompleteBboxStructIsSkipped() {
        ParquetSchema schema = geoSchemaWithBbox("xmin", "ymin", "xmax");

        BboxCoveringPlan plan = resolve(geoOptions().build(), schema);

        assertThat(plan.active()).isFalse();
        assertThat(plan.writtenSchema()).isEqualTo(schema);
    }

    @Test
    void explicitAutoOnBadStructThrows() {
        ParquetSchema schema = geoSchemaWithBbox("xmin", "ymin", "xmax");
        WriteOptions options = geoOptions().bboxCovering(CoveringMode.AUTO).build();

        assertThatThrownBy(() -> resolve(options, schema))
                .isInstanceOf(ParquetWriteException.class)
                .hasMessageContaining("no field 'ymax'");
    }

    @Test
    void autoDeclares3dBboxStruct() {
        ParquetSchema schema = geoSchemaWithBbox("xmin", "xmax", "ymin", "ymax", "zmin", "zmax");

        BboxCoveringPlan plan = resolve(geoOptions().build(), schema);

        assertThat(plan.active()).isTrue();
        assertThat(plan.writtenSchema()).isEqualTo(geoSchemaWithBbox("xmin", "ymin", "zmin", "xmax", "ymax", "zmax"));
        assertThat(plan.xmax()).isEqualTo(ColumnPath.of("bbox", "xmax"));
    }

    @Test
    void loneZminIsSkipped() {
        ParquetSchema schema = geoSchemaWithBbox("xmin", "ymin", "xmax", "ymax", "zmin");

        BboxCoveringPlan plan = resolve(geoOptions().build(), schema);

        assertThat(plan.active()).isFalse();
    }

    @Test
    void extraBboxFieldIsSkipped() {
        ParquetSchema schema = geoSchemaWithBbox("xmin", "ymin", "xmax", "ymax", "area");

        BboxCoveringPlan plan = resolve(geoOptions().build(), schema);

        assertThat(plan.active()).isFalse();
    }

    @Test
    void mixedFloatDoubleIsSkipped() {
        List<SchemaNode> fields = List.of(
                extentLeaf("xmin", PrimitiveKind.FLOAT),
                extentLeaf("ymin", PrimitiveKind.FLOAT),
                extentLeaf("xmax", PrimitiveKind.DOUBLE),
                extentLeaf("ymax", PrimitiveKind.DOUBLE));
        ParquetSchema schema = geoSchemaWithBbox(Repetition.OPTIONAL, fields);
        WriteOptions options = geoOptions().bboxCovering(CoveringMode.AUTO).build();

        assertThatThrownBy(() -> resolve(options, schema))
                .isInstanceOf(ParquetWriteException.class)
                .hasMessageContaining("mix FLOAT and DOUBLE");
    }

    // A required bbox beside an optional geometry breaks the one-to-one rule of the spec.
    @Test
    void bboxRepetitionMustMatch() {
        List<SchemaNode> fields = new ArrayList<>();
        for (String name : List.of("xmin", "ymin", "xmax", "ymax")) {
            fields.add(extentLeaf(name, PrimitiveKind.DOUBLE));
        }
        ParquetSchema schema = geoSchemaWithBbox(Repetition.REQUIRED, fields);
        WriteOptions options = geoOptions().bboxCovering(CoveringMode.AUTO).build();

        assertThatThrownBy(() -> resolve(options, schema))
                .isInstanceOf(ParquetWriteException.class)
                .hasMessageContaining("REQUIRED but the geometry column 'geometry' is OPTIONAL");
    }

    @Test
    void derivedBboxHasSpecShape() {
        BboxCoveringPlan plan = resolve(geoOptions().build(), geoSchema());

        SchemaNode.Group bbox = (SchemaNode.Group)
                plan.writtenSchema().find(ColumnPath.of("bbox")).orElseThrow();
        assertThat(bbox.children()).extracting(SchemaNode::name).containsExactly("xmin", "ymin", "xmax", "ymax");
        assertThat(bbox.repetition())
                .as("the repetition of the optional geometry column")
                .isEqualTo(Repetition.OPTIONAL);
    }

    @Test
    void requiredGeometryRequiredBbox() {
        SchemaNode.Primitive geometry = new SchemaNode.Primitive(
                "geometry", Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        ParquetSchema schema = new ParquetSchema(
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(geometry), Optional.empty(), -1));

        BboxCoveringPlan plan = resolve(geoOptions().build(), schema);

        SchemaNode bbox = plan.writtenSchema().find(ColumnPath.of("bbox")).orElseThrow();
        assertThat(bbox.repetition()).isEqualTo(Repetition.REQUIRED);
    }

    @Test
    void noneIgnoresTheBboxStruct() {
        ParquetSchema schema = geoSchemaWithBbox("xmin", "ymin", "xmax", "ymax");
        WriteOptions options = geoOptions().bboxCovering(CoveringMode.NONE).build();

        BboxCoveringPlan plan = resolve(options, schema);

        assertThat(plan.active()).isFalse();
    }

    // A precision asks for derived values, which would clash with the existing column.
    @Test
    void floatOverBboxStructThrows() {
        ParquetSchema schema = geoSchemaWithBbox("xmin", "ymin", "xmax", "ymax");
        WriteOptions options = geoOptions().bboxCovering(CoveringMode.FLOAT).build();

        assertThatThrownBy(() -> resolve(options, schema))
                .isInstanceOf(ParquetWriteException.class)
                .hasMessageContaining("AUTO");
    }

    /** An optional geometry column and an optional {@code bbox} struct with one DOUBLE leaf per name, in order. */
    private static ParquetSchema geoSchemaWithBbox(String... leafNames) {
        List<SchemaNode> leaves = new ArrayList<>();
        for (String name : leafNames) {
            leaves.add(extentLeaf(name, PrimitiveKind.DOUBLE));
        }
        return geoSchemaWithBbox(Repetition.OPTIONAL, leaves);
    }

    /** An optional geometry column and a {@code bbox} struct of {@code fields}. */
    private static ParquetSchema geoSchemaWithBbox(Repetition bboxRepetition, List<SchemaNode> fields) {
        SchemaNode.Primitive geometry = new SchemaNode.Primitive(
                "geometry", Repetition.OPTIONAL, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group bbox = new SchemaNode.Group("bbox", bboxRepetition, fields, Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(geometry, bbox), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    @Test
    void existingCoveringForAColumnWithoutACrsThrows() {
        ParquetSchema schema = geoSchemaWithExtent(PrimitiveKind.FLOAT);
        WriteOptions options = WriteOptions.builder()
                .existingBboxCovering("geometry", "extent.xmin", "extent.ymin", "extent.xmax", "extent.ymax")
                .build();

        assertThatThrownBy(() -> resolve(options, schema))
                .isInstanceOf(ParquetWriteException.class)
                .hasMessageContaining("geometry");
    }
}
