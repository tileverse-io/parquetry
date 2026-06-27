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
package io.tileverse.parquetry.arrow.ipc;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.FixedLenBinaryVector;
import io.tileverse.parquetry.columnar.Int96Vector;
import io.tileverse.parquetry.columnar.IntSequence;
import io.tileverse.parquetry.columnar.IntVector;
import io.tileverse.parquetry.columnar.ListVector;
import io.tileverse.parquetry.columnar.LongVector;
import io.tileverse.parquetry.columnar.MapVector;
import io.tileverse.parquetry.columnar.ShreddedVariantVector;
import io.tileverse.parquetry.columnar.ShreddedVariantVector.ScalarInput;
import io.tileverse.parquetry.columnar.ShreddedVariantVector.VariantInput;
import io.tileverse.parquetry.columnar.StructVector;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.columnar.VariantVector;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.internal.variant.VariantEncoder;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.variant.ShreddedVariant;

class ArrowExportPrepTest {

    private static final int DECIMAL128_WIDTH = 16;

    @Test
    void dictionaryLeafConsolidates() {
        MemorySegment[] entries = {segment("x"), segment("y")};
        BinaryVector dictionary = BinaryVector.dictionary(entries, IntSequence.of(new int[] {0, 1, 0}), allValid(3));
        ArrowField field = arrowField(leaf("b", PrimitiveKind.BYTE_ARRAY, Optional.empty()));

        ColumnVector prepared = ArrowExportPrep.prepareForExport(dictionary, field);

        assertThat(prepared)
                .isInstanceOfSatisfying(
                        BinaryVector.class, b -> assertThat(b.isDictionary()).isFalse());
    }

    @Test
    void shreddedVariantBecomesVariantVector() {
        ShreddedVariantVector shredded = scalarShredded();
        ArrowField field = arrowField(variantGroup("v"));

        ColumnVector prepared = ArrowExportPrep.prepareForExport(shredded, field);

        assertThat(prepared).isInstanceOf(VariantVector.class);
    }

    @Test
    void int96LeafBecomesLongVector() {
        Int96Vector int96 = Int96Vector.of(MemorySegment.ofArray(new byte[12 * 2]), allValid(2));
        ArrowField field = arrowField(leaf("ts", PrimitiveKind.INT96, Optional.empty()));

        ColumnVector prepared = ArrowExportPrep.prepareForExport(int96, field);

        assertThat(prepared).isInstanceOf(LongVector.class);
    }

    @Test
    void decimalLeafBecomesSixteenByteFixedBinary() {
        IntVector unscaled = IntVector.materialized(new int[] {1234, 5678}, allValid(2));
        ArrowField field = arrowField(leaf("d", PrimitiveKind.INT32, Optional.of(new LogicalType.Decimal(2, 9))));

        ColumnVector prepared = ArrowExportPrep.prepareForExport(unscaled, field);

        assertThat(prepared)
                .isInstanceOfSatisfying(
                        FixedLenBinaryVector.class,
                        fixed -> assertThat(fixed.byteWidth()).isEqualTo(DECIMAL128_WIDTH));
    }

    @Test
    void decimalTransformAppliesInsideAList() {
        IntVector elements = IntVector.materialized(new int[] {10, 20, 30}, allValid(3));
        ListVector list = new ListVector(new int[] {0, 3}, elements, allValid(1), 1);
        ArrowField field = arrowField(listOfDecimal("nums"));

        ColumnVector prepared = ArrowExportPrep.prepareForExport(list, field);

        assertThat(prepared)
                .isInstanceOfSatisfying(
                        ListVector.class,
                        listVector -> assertThat(listVector.child()).isInstanceOf(FixedLenBinaryVector.class));
    }

    @Test
    void int96TransformAppliesInsideAStruct() {
        Map<ColumnPath, ColumnVector> children = new LinkedHashMap<>();
        children.put(ColumnPath.of("ts"), Int96Vector.of(MemorySegment.ofArray(new byte[12]), allValid(1)));
        StructVector struct = new StructVector(children, allValid(1), 1);
        ArrowField field = arrowField(structWithInt96("s"));

        ColumnVector prepared = ArrowExportPrep.prepareForExport(struct, field);

        assertThat(prepared)
                .isInstanceOfSatisfying(
                        StructVector.class,
                        structVector -> assertThat(structVector.children().get(ColumnPath.of("ts")))
                                .isInstanceOf(LongVector.class));
    }

    @Test
    void mapChildrenAreConsolidated() {
        BinaryVector keys = BinaryVector.materialized(new MemorySegment[] {segment("k")}, allValid(1));
        LongVector values = LongVector.materialized(new long[] {42L}, allValid(1));
        MapVector map = new MapVector(new int[] {0, 1}, keys, values, allValid(1), 1);
        ArrowField field = arrowField(mapGroup("props"));

        ColumnVector prepared = ArrowExportPrep.prepareForExport(map, field);

        assertThat(prepared).isInstanceOfSatisfying(MapVector.class, mapVector -> {
            assertThat(mapVector.keys()).isInstanceOf(BinaryVector.class);
            assertThat(mapVector.values()).isInstanceOf(LongVector.class);
        });
    }

    private static ShreddedVariantVector scalarShredded() {
        VariantEncoder.Encoded[] references = {
            new VariantEncoder().addLong(7L).encode(),
            new VariantEncoder().addLong(9L).encode()
        };
        BinaryVector metadataColumn = sharedMetadata(references);
        ShreddedVariant.Scalar model = new ShreddedVariant.Scalar(
                new SchemaNode.Primitive(
                        "typed_value",
                        Repetition.OPTIONAL,
                        PrimitiveKind.INT64,
                        OptionalInt.empty(),
                        Optional.empty(),
                        -1),
                6);
        LongVector typed = LongVector.materialized(new long[] {7L, 9L}, allValid(2));
        VariantInput root = new VariantInput(allNullBinary(2), new ScalarInput(typed));
        return new ShreddedVariantVector(metadataColumn, model, root, allValid(2), 2);
    }

    private static BinaryVector sharedMetadata(VariantEncoder.Encoded[] references) {
        MemorySegment[] perRow = new MemorySegment[references.length];
        for (int row = 0; row < references.length; row++) {
            perRow[row] = references[row].metadata();
        }
        return BinaryVector.materialized(perRow, allValid(references.length));
    }

    private static BinaryVector allNullBinary(int rows) {
        return BinaryVector.materialized(new MemorySegment[rows], Validity.of(new BitSet(rows), rows));
    }

    private static ArrowField arrowField(SchemaNode node) {
        ParquetSchema schema = new ParquetSchema(
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(node), Optional.empty(), -1));
        return ArrowField.of(node, GeoArrowFields.resolve(schema, Optional.empty()));
    }

    private static SchemaNode.Primitive leaf(String name, PrimitiveKind kind, Optional<LogicalType> logical) {
        return new SchemaNode.Primitive(name, Repetition.OPTIONAL, kind, OptionalInt.empty(), logical, 0);
    }

    private static SchemaNode.Group listOfDecimal(String name) {
        SchemaNode.Primitive element = leaf("element", PrimitiveKind.INT32, Optional.of(new LogicalType.Decimal(2, 9)));
        SchemaNode.Group wrapper =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        return new SchemaNode.Group(
                name, Repetition.OPTIONAL, List.of(wrapper), Optional.of(new LogicalType.ListType()), 1);
    }

    private static SchemaNode.Group structWithInt96(String name) {
        SchemaNode.Primitive ts = leaf("ts", PrimitiveKind.INT96, Optional.empty());
        return new SchemaNode.Group(name, Repetition.OPTIONAL, List.of(ts), Optional.empty(), 1);
    }

    private static SchemaNode.Group mapGroup(String name) {
        SchemaNode.Primitive key = leaf("key", PrimitiveKind.BYTE_ARRAY, Optional.of(new LogicalType.StringType()));
        SchemaNode.Primitive value = leaf("value", PrimitiveKind.INT64, Optional.empty());
        SchemaNode.Group keyValue =
                new SchemaNode.Group("key_value", Repetition.REPEATED, List.of(key, value), Optional.empty(), -1);
        return new SchemaNode.Group(
                name, Repetition.OPTIONAL, List.of(keyValue), Optional.of(new LogicalType.MapType()), 1);
    }

    private static SchemaNode.Group variantGroup(String name) {
        SchemaNode.Primitive metadata = leaf("metadata", PrimitiveKind.BYTE_ARRAY, Optional.empty());
        SchemaNode.Primitive value = leaf("value", PrimitiveKind.BYTE_ARRAY, Optional.empty());
        return new SchemaNode.Group(
                name, Repetition.OPTIONAL, List.of(metadata, value), Optional.of(new LogicalType.Variant()), 1);
    }

    private static MemorySegment segment(String text) {
        return MemorySegment.ofArray(text.getBytes(StandardCharsets.UTF_8));
    }

    private static Validity allValid(int rows) {
        return Validity.allValid(rows);
    }
}
