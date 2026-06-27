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
package io.tileverse.parquetry.columnar;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.data.variant.Variant;
import io.tileverse.parquetry.data.variant.VariantEncoder;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class VariantVectorTest {

    private static final ColumnPath VARIANT_COLUMN = ColumnPath.of("v");

    @Nested
    class RecordAccess {

        @Test
        void getReturnsNavigableVariant() {
            VariantEncoder.Encoded encoded = singleFieldObject("n", 42);
            VariantVector variantVec = variantVector(encoded, validBit());

            ParquetSchema schema = variantSchema();
            Map<ColumnPath, ColumnVector> columns = Map.of(VARIANT_COLUMN, variantVec);
            try (ParquetRecordBatch batch = new DefaultParquetRecordBatch(schema, columns, 1, Arena.ofConfined())) {
                ParquetRecord row = batch.materialize(0);
                Object cell = row.get(VARIANT_COLUMN);
                assertThat(cell).as("variant cell type").isInstanceOf(Variant.class);
                assertThat(((Variant) cell).getField("n").getInt())
                        .as("navigated value")
                        .isEqualTo(42);
            }
        }

        @Test
        void nullRowReturnsNull() {
            VariantEncoder.Encoded encoded = singleFieldObject("n", 42);
            VariantVector variantVec = variantVector(encoded, Validity.of(new BitSet(1), 1));

            ParquetSchema schema = variantSchema();
            Map<ColumnPath, ColumnVector> columns = Map.of(VARIANT_COLUMN, variantVec);
            try (ParquetRecordBatch batch = new DefaultParquetRecordBatch(schema, columns, 1, Arena.ofConfined())) {
                ParquetRecord row = batch.materialize(0);
                assertThat(row.get(VARIANT_COLUMN))
                        .as("cleared validity yields null")
                        .isNull();
            }
        }
    }

    @Nested
    class Detach {

        @Test
        void detachedVariantOutlivesBatch() {
            VariantEncoder.Encoded encoded = singleFieldObject("n", 42);
            ParquetSchema schema = variantSchema();
            Arena arena = Arena.ofConfined();
            VariantVector variantVec = arenaBackedVariantVector(encoded, validBit(), arena);
            Map<ColumnPath, ColumnVector> columns = Map.of(VARIANT_COLUMN, variantVec);

            Variant live;
            Variant detached;
            try (ParquetRecordBatch batch = new DefaultParquetRecordBatch(schema, columns, 1, arena)) {
                ParquetRecord row = batch.materialize(0);
                live = (Variant) row.get(VARIANT_COLUMN);
                detached = live.detach();
            }

            assertThat(detached.getField("n").getInt())
                    .as("detached copy navigates after the batch arena is closed")
                    .isEqualTo(42);
            // The binary leaves consolidate into a heap backing buffer at construction, decoupling them from any
            // decode arena, which is why a variant read still succeeds once that arena is closed.
            assertThat(live.getField("n").getInt())
                    .as("consolidated binary leaves are heap-owned and outlive the decode arena")
                    .isEqualTo(42);
        }
    }

    private static VariantEncoder.Encoded singleFieldObject(String field, int value) {
        VariantEncoder encoder = new VariantEncoder();
        encoder.startObject();
        encoder.field(field).addInt(value);
        encoder.endObject();
        return encoder.encode();
    }

    private static VariantVector variantVector(VariantEncoder.Encoded encoded, Validity validity) {
        BinaryVector metadataVec = BinaryVector.materialized(new MemorySegment[] {encoded.metadata()}, validity);
        BinaryVector valueVec = BinaryVector.materialized(new MemorySegment[] {encoded.value()}, validity);
        return new VariantVector(metadataVec, valueVec, validity, 1);
    }

    // Sources the variant's leaves from arena-allocated segments to confirm that consolidation copies their bytes
    // into a heap backing buffer, leaving the resulting vector independent of that arena.
    private static VariantVector arenaBackedVariantVector(
            VariantEncoder.Encoded encoded, Validity validity, Arena arena) {
        BinaryVector metadataVec =
                BinaryVector.materialized(new MemorySegment[] {copyInto(arena, encoded.metadata())}, validity);
        BinaryVector valueVec =
                BinaryVector.materialized(new MemorySegment[] {copyInto(arena, encoded.value())}, validity);
        return new VariantVector(metadataVec, valueVec, validity, 1);
    }

    private static MemorySegment copyInto(Arena arena, MemorySegment source) {
        MemorySegment target = arena.allocate(source.byteSize());
        MemorySegment.copy(source, 0, target, 0, source.byteSize());
        return target.asReadOnly();
    }

    private static Validity validBit() {
        return Validity.allValid(1);
    }

    private static ParquetSchema variantSchema() {
        SchemaNode metadata = new SchemaNode.Primitive(
                "metadata", Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode value = new SchemaNode.Primitive(
                "value", Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode variant = new SchemaNode.Group(
                "v", Repetition.OPTIONAL, List.of(metadata, value), Optional.of(new LogicalType.Variant()), -1);
        SchemaNode root = new SchemaNode.Group("root", Repetition.REQUIRED, List.of(variant), Optional.empty(), -1);
        return new ParquetSchema((SchemaNode.Group) root);
    }
}
