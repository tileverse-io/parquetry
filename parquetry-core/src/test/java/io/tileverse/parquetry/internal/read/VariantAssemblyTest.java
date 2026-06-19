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
package io.tileverse.parquetry.internal.read;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.batch.BinaryVector;
import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.LongVector;
import io.tileverse.parquetry.batch.ShreddedVariantVector;
import io.tileverse.parquetry.batch.Validity;
import io.tileverse.parquetry.batch.VariantVector;
import io.tileverse.parquetry.data.variant.Variant;
import io.tileverse.parquetry.data.variant.VariantEncoder;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class VariantAssemblyTest {

    @Test
    void assemblesVariantGroupIntoVariantVector() {
        VariantEncoder.Encoded encoded = singleFieldObject("n", 7);
        Map<ColumnPath, ColumnVector> leafVectors = Map.of(
                ColumnPath.of("v", "metadata"), oneRow(encoded.metadata()),
                ColumnPath.of("v", "value"), oneRow(encoded.value()));

        Map<ColumnPath, ColumnVector> assembled =
                NestedVectorAssembler.assembleNested(variantSchema(), leafVectors, Map.of(), Map.of(), 1);

        ColumnVector wrapper = assembled.get(ColumnPath.of("v"));
        assertThat(wrapper).as("assembled wrapper kind").isInstanceOf(VariantVector.class);
        Variant value = ((VariantVector) wrapper).get(0);
        assertThat(value.getField("n").getInt())
                .as("navigated value through the assembled wrapper")
                .isEqualTo(7);
    }

    @Test
    void shreddedVariantAssemblesIntoShreddedVector() {
        VariantEncoder.Encoded encoded = singleFieldObject("n", 7);
        long[] typedValues = {42L};
        Map<ColumnPath, ColumnVector> leafVectors = Map.of(
                ColumnPath.of("v", "metadata"), oneRow(encoded.metadata()),
                ColumnPath.of("v", "value"), allNullBinaryRow(),
                ColumnPath.of("v", "typed_value"),
                        LongVector.materialized(typedValues, Validity.allValid(typedValues.length)));

        Map<ColumnPath, ColumnVector> assembled =
                NestedVectorAssembler.assembleNested(shreddedScalarVariantSchema(), leafVectors, Map.of(), Map.of(), 1);

        ColumnVector wrapper = assembled.get(ColumnPath.of("v"));
        assertThat(wrapper).as("assembled wrapper kind").isInstanceOf(ShreddedVariantVector.class);
        Variant value = ((ShreddedVariantVector) wrapper).get(0);
        assertThat(value.getLong())
                .as("scalar typed_value reconstructed through the shredded wrapper")
                .isEqualTo(42L);
    }

    private static VariantEncoder.Encoded singleFieldObject(String field, int value) {
        VariantEncoder encoder = new VariantEncoder();
        encoder.startObject();
        encoder.field(field).addInt(value);
        encoder.endObject();
        return encoder.encode();
    }

    private static BinaryVector oneRow(MemorySegment bytes) {
        BitSet valid = new BitSet(1);
        valid.set(0);
        return BinaryVector.materialized(new MemorySegment[] {bytes}, Validity.of(valid, 1));
    }

    private static BinaryVector allNullBinaryRow() {
        return BinaryVector.materialized(new MemorySegment[1], Validity.of(new BitSet(1), 1));
    }

    private static ParquetSchema variantSchema() {
        return schemaWith(List.of(binaryLeaf("metadata"), binaryLeaf("value")));
    }

    private static ParquetSchema shreddedScalarVariantSchema() {
        return schemaWith(List.of(
                binaryLeaf("metadata"),
                optionalBinaryLeaf("value"),
                new SchemaNode.Primitive(
                        "typed_value",
                        Repetition.OPTIONAL,
                        PrimitiveKind.INT64,
                        OptionalInt.empty(),
                        Optional.empty(),
                        -1)));
    }

    private static ParquetSchema schemaWith(List<SchemaNode> variantChildren) {
        SchemaNode variant = new SchemaNode.Group(
                "v", Repetition.OPTIONAL, variantChildren, Optional.of(new LogicalType.Variant()), -1);
        SchemaNode root = new SchemaNode.Group("root", Repetition.REQUIRED, List.of(variant), Optional.empty(), -1);
        return new ParquetSchema((SchemaNode.Group) root);
    }

    private static SchemaNode binaryLeaf(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode optionalBinaryLeaf(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.OPTIONAL, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
    }
}
