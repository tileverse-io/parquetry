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

    @Test
    void v2OnlyDefaultResolvesToNoCovering() {
        WriteOptions options = geoOptions()
                .geoParquetMetadata(GeoParquetMetadataMode.V2_0_ONLY)
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
        SchemaNode.Primitive geometry = new SchemaNode.Primitive(
                "geometry", Repetition.OPTIONAL, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        List<SchemaNode> extentLeaves = List.of(
                extentLeaf("xmin", extentKind),
                extentLeaf("ymin", extentKind),
                extentLeaf("xmax", extentKind),
                extentLeaf("ymax", extentKind));
        SchemaNode.Group extent =
                new SchemaNode.Group("extent", Repetition.REQUIRED, extentLeaves, Optional.empty(), -1);
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
                .hasMessageContaining("extent.xmin");
    }

    @Test
    void existingCoveringOverANonNumericLeafThrows() {
        ParquetSchema schema = geoSchemaWithExtent(PrimitiveKind.INT32);
        WriteOptions options = existingExtentCovering().build();

        assertThatThrownBy(() -> resolve(options, schema))
                .isInstanceOf(ParquetWriteException.class)
                .hasMessageContaining("FLOAT or DOUBLE");
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
