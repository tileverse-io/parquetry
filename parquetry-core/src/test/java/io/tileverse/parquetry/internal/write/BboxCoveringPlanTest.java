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

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

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
        assertThatThrownBy(() -> resolve(options, geoSchema()))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("geometry");
    }
}
