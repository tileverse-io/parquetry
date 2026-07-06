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

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.internal.variant.VariantEncoder;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.variant.ShreddedVariant;

class FilteredRecordBatchTest {

    private static final ColumnPath A = ColumnPath.of("a");
    private static final ColumnPath B = ColumnPath.of("b");
    private static final ColumnPath TAGS = ColumnPath.of("tags");
    private static final ColumnPath V = ColumnPath.of("v");
    private static final ColumnPath S = ColumnPath.of("s");

    @Test
    void filteredViewKeepsOnlyTheSetRowsAndTheOutputColumns() {
        ParquetRecordBatch source = intBatch(Map.of(A, new int[] {10, 11, 12, 13}, B, new int[] {20, 21, 22, 23}));
        BitSet keep = new BitSet();
        keep.set(1);
        keep.set(3);

        ParquetRecordBatch result = FilteredRecordBatch.filtered(source, keep, schemaOf(A));
        try {
            assertThat(result.rowCount()).isEqualTo(2);
            assertThat(result.columns().keySet()).containsExactly(A);
            IntVector survivingA = (IntVector) result.columns().get(A);
            assertThat(survivingA.getInt(0)).isEqualTo(11);
            assertThat(survivingA.getInt(1)).isEqualTo(13);
        } finally {
            result.close();
        }
    }

    @Test
    void projectDropsNonOutputColumnsButKeepsEveryRow() {
        ParquetRecordBatch source = intBatch(Map.of(A, new int[] {10, 11, 12}, B, new int[] {20, 21, 22}));

        ParquetRecordBatch result = FilteredRecordBatch.narrowed(source, schemaOf(A));
        try {
            assertThat(result.rowCount()).isEqualTo(3);
            assertThat(result.columns().keySet()).containsExactly(A);
        } finally {
            result.close();
        }
    }

    @Test
    void projectReturnsTheSourceUnchangedWhenAlreadyOutputShaped() {
        ParquetRecordBatch source = intBatch(Map.of(A, new int[] {10, 11, 12}));

        ParquetRecordBatch result = FilteredRecordBatch.narrowed(source, schemaOf(A));
        try {
            assertThat(result).isSameAs(source);
        } finally {
            result.close();
        }
    }

    @Test
    void filteredViewKeepsAllRowsWhenEveryBitIsSet() {
        ParquetRecordBatch source = intBatch(Map.of(A, new int[] {10, 11, 12, 13}));
        BitSet keep = new BitSet();
        keep.set(0, 4);

        ParquetRecordBatch result = FilteredRecordBatch.filtered(source, keep, schemaOf(A));
        try {
            assertThat(result.rowCount()).isEqualTo(4);
            IntVector survivingA = (IntVector) result.columns().get(A);
            assertThat(survivingA.getInt(0)).isEqualTo(10);
            assertThat(survivingA.getInt(1)).isEqualTo(11);
            assertThat(survivingA.getInt(2)).isEqualTo(12);
            assertThat(survivingA.getInt(3)).isEqualTo(13);
        } finally {
            result.close();
        }
    }

    @Test
    void closingTheSurvivorClosesTheSource() {
        ParquetRecordBatch source = intBatch(Map.of(A, new int[] {10, 11, 12, 13}, B, new int[] {20, 21, 22, 23}));
        FlagCloseable sourceClosed = new FlagCloseable();
        source.registerBuffer(sourceClosed);

        BitSet keep = new BitSet();
        keep.set(1);
        keep.set(3);
        ParquetRecordBatch survivor = FilteredRecordBatch.filtered(source, keep, schemaOf(A));

        survivor.close();

        assertThat(sourceClosed.closed).isTrue();
    }

    @Test
    void compactedReturnsThisWhenOnlyNarrowed() {
        ParquetRecordBatch source = intBatch(Map.of(A, new int[] {10, 11, 12}, B, new int[] {20, 21, 22}));

        FilteredRecordBatch view = (FilteredRecordBatch) FilteredRecordBatch.narrowed(source, schemaOf(A));
        try {
            assertThat(view.compacted()).isSameAs(view);
        } finally {
            view.close();
        }
    }

    @Test
    void compactedGathersSurvivorsToADenseBatch() {
        ParquetRecordBatch source = intBatch(Map.of(A, new int[] {10, 11, 12, 13}, B, new int[] {20, 21, 22, 23}));
        BitSet keep = new BitSet();
        keep.set(1);
        keep.set(3);

        FilteredRecordBatch view = (FilteredRecordBatch) FilteredRecordBatch.filtered(source, keep, schemaOf(A));
        try (ParquetRecordBatch dense = view.compacted()) {
            assertThat(dense).isInstanceOf(DefaultParquetRecordBatch.class);
            assertThat(dense.rowCount()).isEqualTo(2);
            assertThat(dense.columns().keySet()).containsExactly(A);
            IntVector a = (IntVector) dense.columns().get(A);
            assertThat(a.getInt(0)).isEqualTo(11);
            assertThat(a.getInt(1)).isEqualTo(13);
        } finally {
            view.close();
        }
    }

    @Test
    void compactedProjectsTheNullMaskThroughTheSelection() {
        IntVector column = IntVector.materialized(new int[] {10, 11, 12, 13}, validityOf("1010"));
        ParquetRecordBatch source = DefaultParquetRecordBatch.ofHeap(schemaOf(A), Map.of(A, (ColumnVector) column), 4);
        BitSet keep = new BitSet();
        keep.set(1);
        keep.set(2);

        FilteredRecordBatch view = (FilteredRecordBatch) FilteredRecordBatch.filtered(source, keep, schemaOf(A));
        try (ParquetRecordBatch dense = view.compacted()) {
            ColumnVector a = dense.columns().get(A);
            assertThat(a.validity().isValid(0)).isFalse();
            assertThat(a.validity().isValid(1)).isTrue();
        } finally {
            view.close();
        }
    }

    @Test
    void compactedReusesTheVectorWhenEveryRowSurvives() {
        ParquetRecordBatch source = intBatch(Map.of(A, new int[] {10, 11, 12}, B, new int[] {20, 21, 22}));
        BitSet keep = new BitSet();
        keep.set(0, 3);

        FilteredRecordBatch view = (FilteredRecordBatch) FilteredRecordBatch.filtered(source, keep, schemaOf(A));
        try (ParquetRecordBatch dense = view.compacted()) {
            assertThat(dense.columns().get(A)).isSameAs(source.columns().get(A));
        } finally {
            view.close();
        }
    }

    @Test
    void compactedGathersARangeSlice() {
        ParquetRecordBatch source = intBatch(Map.of(A, new int[] {10, 11, 12, 13}));

        ParquetRecordBatch slice = source.slice(1, 2);
        try (ParquetRecordBatch dense = ((FilteredRecordBatch) slice).compacted()) {
            assertThat(dense.rowCount()).isEqualTo(2);
            IntVector a = (IntVector) dense.columns().get(A);
            assertThat(a.getInt(0)).isEqualTo(11);
            assertThat(a.getInt(1)).isEqualTo(12);
        } finally {
            slice.close();
        }
    }

    @Test
    void compactedGathersAListColumn() {
        // rows: [1,2] / [] / [3] / [4,5,6]; keep rows 0 and 3
        int[] offsets = {0, 2, 2, 3, 6};
        IntVector elements = IntVector.materialized(new int[] {1, 2, 3, 4, 5, 6}, Validity.allValid(6));
        ListVector tags = new ListVector(offsets, elements, Validity.allValid(4), 4);
        ParquetRecordBatch source =
                DefaultParquetRecordBatch.ofHeap(listSchema(), Map.of(TAGS, (ColumnVector) tags), 4);
        BitSet keep = new BitSet();
        keep.set(0);
        keep.set(3);

        FilteredRecordBatch view = (FilteredRecordBatch) FilteredRecordBatch.filtered(source, keep, listSchema());
        try (ParquetRecordBatch dense = view.compacted()) {
            ListVector gathered = (ListVector) dense.columns().get(TAGS);
            assertThat(dense.rowCount()).isEqualTo(2);
            assertThat(gathered.offsets()).containsExactly(0, 2, 5);
            IntVector child = (IntVector) gathered.child();
            assertThat(child.getInt(0)).isEqualTo(1);
            assertThat(child.getInt(1)).isEqualTo(2);
            assertThat(child.getInt(2)).isEqualTo(4);
            assertThat(child.getInt(3)).isEqualTo(5);
            assertThat(child.getInt(4)).isEqualTo(6);
        } finally {
            view.close();
        }
    }

    @Test
    void compactedUnshredsAShreddedVariantColumn() {
        ShreddedVariantVector shredded = twoRowShreddedVariant();
        VariantVector expected = shredded.toUnshredded();
        ParquetRecordBatch source =
                DefaultParquetRecordBatch.ofHeap(variantSchema(), Map.of(V, (ColumnVector) shredded), 2);
        BitSet keep = new BitSet();
        keep.set(1);

        FilteredRecordBatch view = (FilteredRecordBatch) FilteredRecordBatch.filtered(source, keep, variantSchema());
        try (ParquetRecordBatch dense = view.compacted()) {
            VariantVector actual = (VariantVector) dense.columns().get(V);
            assertThat(actual.size()).isEqualTo(1);
            assertThat(bytesOf(actual.valueColumn().get(0)))
                    .isEqualTo(bytesOf(expected.valueColumn().get(1)));
            assertThat(bytesOf(actual.metadataColumn().get(0)))
                    .isEqualTo(bytesOf(expected.metadataColumn().get(1)));
        } finally {
            view.close();
        }
    }

    @Test
    void compactedUnshredsAShreddedVariantNestedInAStruct() {
        ShreddedVariantVector shredded = twoRowShreddedVariant();
        StructVector wrapper =
                new StructVector(Map.of(ColumnPath.of("v"), (ColumnVector) shredded), Validity.allValid(2), 2);
        ParquetRecordBatch source =
                DefaultParquetRecordBatch.ofHeap(structSchema(), Map.of(S, (ColumnVector) wrapper), 2);
        BitSet keep = new BitSet();
        keep.set(0);

        FilteredRecordBatch view = (FilteredRecordBatch) FilteredRecordBatch.filtered(source, keep, structSchema());
        try (ParquetRecordBatch dense = view.compacted()) {
            StructVector gathered = (StructVector) dense.columns().get(S);
            assertThat(gathered.size()).isEqualTo(1);
            assertThat(gathered.children().get(ColumnPath.of("v"))).isInstanceOf(VariantVector.class);
        } finally {
            view.close();
        }
    }

    private static final class FlagCloseable implements AutoCloseable {
        private boolean closed;

        @Override
        public void close() {
            closed = true;
        }
    }

    private static ParquetRecordBatch intBatch(Map<ColumnPath, int[]> columnValues) {
        int rowCount = columnValues.values().iterator().next().length;
        Map<ColumnPath, ColumnVector> columns = LinkedHashMap.newLinkedHashMap(columnValues.size());
        for (Map.Entry<ColumnPath, int[]> entry : columnValues.entrySet()) {
            columns.put(entry.getKey(), IntVector.materialized(entry.getValue(), Validity.allValid(rowCount)));
        }
        return DefaultParquetRecordBatch.ofHeap(
                schemaOf(columnValues.keySet().toArray(ColumnPath[]::new)), columns, rowCount);
    }

    private static ParquetSchema schemaOf(ColumnPath... leaves) {
        List<SchemaNode> children = new ArrayList<>(leaves.length);
        for (ColumnPath leaf : leaves) {
            children.add(new SchemaNode.Primitive(
                    leaf.dot(), Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1));
        }
        return new ParquetSchema(new SchemaNode.Group("root", Repetition.REQUIRED, children, Optional.empty(), -1));
    }

    private static Validity validityOf(String bits) {
        BitSet valid = new BitSet(bits.length());
        for (int i = 0; i < bits.length(); i++) {
            if (bits.charAt(i) == '1') {
                valid.set(i);
            }
        }
        return Validity.of(valid, bits.length());
    }

    private static byte[] bytesOf(MemorySegment segment) {
        return segment.toArray(ValueLayout.JAVA_BYTE);
    }

    /** A 2-row scalar-shredded Variant column: typed_value INT64 {7, 11}, value leaf all-null, shared metadata. */
    private static ShreddedVariantVector twoRowShreddedVariant() {
        long[] typedValues = {7L, 11L};
        MemorySegment metadata =
                new VariantEncoder().addLong(typedValues[0]).encode().metadata();
        MemorySegment[] metadataPerRow = {metadata, metadata};
        BinaryVector metadataColumn = BinaryVector.materialized(metadataPerRow, Validity.allValid(2));
        BinaryVector valueLeaf = BinaryVector.materialized(new MemorySegment[2], Validity.of(new BitSet(), 2));
        LongVector typedColumn = LongVector.materialized(typedValues, Validity.allValid(2));
        ShreddedVariantVector.VariantInput root =
                new ShreddedVariantVector.VariantInput(valueLeaf, new ShreddedVariantVector.ScalarInput(typedColumn));
        SchemaNode.Primitive typedValue = new SchemaNode.Primitive(
                "typed_value", Repetition.OPTIONAL, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
        ShreddedVariant.Scalar model = new ShreddedVariant.Scalar(typedValue, 6);
        return new ShreddedVariantVector(metadataColumn, model, root, Validity.allValid(2), 2);
    }

    private static ParquetSchema listSchema() {
        SchemaNode.Primitive element = new SchemaNode.Primitive(
                "element", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group list =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        SchemaNode.Group tags = new SchemaNode.Group(
                "tags", Repetition.OPTIONAL, List.of(list), Optional.of(new LogicalType.ListType()), -1);
        return new ParquetSchema(
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(tags), Optional.empty(), -1));
    }

    private static ParquetSchema variantSchema() {
        SchemaNode.Primitive metadata = new SchemaNode.Primitive(
                "metadata", Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive value = new SchemaNode.Primitive(
                "value", Repetition.OPTIONAL, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group v =
                new SchemaNode.Group("v", Repetition.OPTIONAL, List.of(metadata, value), Optional.empty(), -1);
        return new ParquetSchema(new SchemaNode.Group("root", Repetition.REQUIRED, List.of(v), Optional.empty(), -1));
    }

    private static ParquetSchema structSchema() {
        SchemaNode.Primitive metadata = new SchemaNode.Primitive(
                "metadata", Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive value = new SchemaNode.Primitive(
                "value", Repetition.OPTIONAL, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group v =
                new SchemaNode.Group("v", Repetition.OPTIONAL, List.of(metadata, value), Optional.empty(), -1);
        SchemaNode.Group s = new SchemaNode.Group("s", Repetition.OPTIONAL, List.of(v), Optional.empty(), -1);
        return new ParquetSchema(new SchemaNode.Group("root", Repetition.REQUIRED, List.of(s), Optional.empty(), -1));
    }
}
