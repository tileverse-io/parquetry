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
package io.tileverse.parquetry.arrow.columnar;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.SequencedMap;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.IntSequence;
import io.tileverse.parquetry.columnar.IntVector;
import io.tileverse.parquetry.columnar.ListVector;
import io.tileverse.parquetry.columnar.MapVector;
import io.tileverse.parquetry.columnar.StructVector;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.columnar.VariantVector;
import io.tileverse.parquetry.schema.ColumnPath;

class ArrowBufferCodecNestedTest {

    private static final ColumnType INT = new ColumnType.Primitive(LeafType.INT32);
    private static final ColumnType BIN = new ColumnType.Primitive(LeafType.BINARY);

    @Test
    void listOfIntRoundTripsWithNullAndEmptyRows() {
        // Rows: [10, 20], null, [], [30]. Child holds the three present values 10, 20, 30.
        IntVector child = IntVector.materialized(new int[] {10, 20, 30}, Validity.allValid(3));
        int[] offsets = {0, 2, 2, 2, 3};
        BitSet valid = new BitSet(4);
        valid.set(0);
        valid.set(2);
        valid.set(3); // row 1 null
        ListVector original = new ListVector(offsets, child, Validity.of(valid, 4), 4);

        ColumnType type = new ColumnType.ListType(INT);
        ListVector restored = (ListVector) roundTrip(original, type);

        assertThat(restored.size()).isEqualTo(4);
        assertThat(restored.offsets()).containsExactly(0, 2, 2, 2, 3);
        assertThat(restored.validity().isNull(1)).isTrue();
        assertThat(restored.validity().isValid(2)).isTrue();
        IntVector restoredChild = (IntVector) restored.child();
        assertThat(restoredChild.size()).isEqualTo(3);
        assertThat(restoredChild.getInt(0)).isEqualTo(10);
        assertThat(restoredChild.getInt(1)).isEqualTo(20);
        assertThat(restoredChild.getInt(2)).isEqualTo(30);
        // Row 2 is an empty list: its slice is offsets[2]..offsets[3] == 2..2.
        assertThat(restored.rowOffsetEnd(2) - restored.rowOffsetStart(2)).isZero();
    }

    @Test
    void structOfTwoLeavesRoundTripsWithFieldNull() {
        IntVector ages = IntVector.materialized(new int[] {7, 0, 9}, ageValidity());
        BinaryVector names = namesVector();
        SequencedMap<ColumnPath, ColumnVector> children = new LinkedHashMap<>();
        children.put(ColumnPath.of("age"), ages);
        children.put(ColumnPath.of("name"), names);
        StructVector original = new StructVector(children, Validity.allValid(3), 3);

        SequencedMap<ColumnPath, ColumnType> fields = new LinkedHashMap<>();
        fields.put(ColumnPath.of("age"), INT);
        fields.put(ColumnPath.of("name"), BIN);
        ColumnType type = new ColumnType.StructType(fields);
        StructVector restored = (StructVector) roundTrip(original, type);

        assertThat(restored.size()).isEqualTo(3);
        IntVector restoredAges = (IntVector) restored.children().get(ColumnPath.of("age"));
        assertThat(restoredAges.getInt(0)).isEqualTo(7);
        assertThat(restoredAges.isNull(1)).isTrue();
        assertThat(restoredAges.getInt(2)).isEqualTo(9);
        BinaryVector restoredNames = (BinaryVector) restored.children().get(ColumnPath.of("name"));
        assertThat(bytesAt(restoredNames, 0)).containsExactly(utf8("ann"));
        assertThat(bytesAt(restoredNames, 1)).containsExactly(utf8("bob"));
        assertThat(bytesAt(restoredNames, 2)).containsExactly(utf8("cleo"));
    }

    @Test
    void mapOfIntKeysToBinaryValuesRoundTrips() {
        // Rows: {1->"x", 2->"yy"}, {}, {3->"zzz"}. Flattened keys/values hold the three entries.
        IntVector keys = IntVector.materialized(new int[] {1, 2, 3}, Validity.allValid(3));
        BinaryVector values = mapValuesVector();
        int[] offsets = {0, 2, 2, 3};
        MapVector original = new MapVector(offsets, keys, values, Validity.allValid(3), 3);

        ColumnType type = new ColumnType.MapType(INT, BIN);
        MapVector restored = (MapVector) roundTrip(original, type);

        assertThat(restored.size()).isEqualTo(3);
        assertThat(restored.offsets()).containsExactly(0, 2, 2, 3);
        IntVector restoredKeys = (IntVector) restored.keys();
        assertThat(restoredKeys.getInt(0)).isEqualTo(1);
        assertThat(restoredKeys.getInt(1)).isEqualTo(2);
        assertThat(restoredKeys.getInt(2)).isEqualTo(3);
        BinaryVector restoredValues = (BinaryVector) restored.values();
        assertThat(bytesAt(restoredValues, 0)).containsExactly(utf8("x"));
        assertThat(bytesAt(restoredValues, 1)).containsExactly(utf8("yy"));
        assertThat(bytesAt(restoredValues, 2)).containsExactly(utf8("zzz"));
    }

    @Test
    void variantRoundTripsWithNullRow() {
        BinaryVector metadata = variantMetadataVector();
        BinaryVector value = variantValueVector();
        BitSet valid = new BitSet(3);
        valid.set(0);
        valid.set(2); // row 1 null
        VariantVector original = new VariantVector(metadata, value, Validity.of(valid, 3), 3);

        ColumnType type = new ColumnType.VariantType();
        VariantVector restored = (VariantVector) roundTrip(original, type);

        assertThat(restored.size()).isEqualTo(3);
        assertThat(restored.validity().isNull(1)).isTrue();
        assertThat(bytesAt(restored.metadataColumn(), 0)).containsExactly((byte) 1, (byte) 0, (byte) 0);
        assertThat(bytesAt(restored.metadataColumn(), 2)).containsExactly((byte) 1, (byte) 0, (byte) 0);
        assertThat(bytesAt(restored.valueColumn(), 0)).containsExactly((byte) 12);
        assertThat(bytesAt(restored.valueColumn(), 2)).containsExactly((byte) 44);
    }

    @Test
    void listOfStructRoundTrips() {
        // Two rows: row 0 holds two structs, row 1 holds one struct. Three entry structs total.
        IntVector ids = IntVector.materialized(new int[] {100, 200, 300}, Validity.allValid(3));
        SequencedMap<ColumnPath, ColumnVector> entryChildren = new LinkedHashMap<>();
        entryChildren.put(ColumnPath.of("id"), ids);
        StructVector entries = new StructVector(entryChildren, Validity.allValid(3), 3);
        int[] offsets = {0, 2, 3};
        ListVector original = new ListVector(offsets, entries, Validity.allValid(2), 2);

        SequencedMap<ColumnPath, ColumnType> entryFields = new LinkedHashMap<>();
        entryFields.put(ColumnPath.of("id"), INT);
        ColumnType type = new ColumnType.ListType(new ColumnType.StructType(entryFields));
        ListVector restored = (ListVector) roundTrip(original, type);

        assertThat(restored.size()).isEqualTo(2);
        assertThat(restored.offsets()).containsExactly(0, 2, 3);
        StructVector restoredEntries = (StructVector) restored.child();
        assertThat(restoredEntries.size()).isEqualTo(3);
        IntVector restoredIds = (IntVector) restoredEntries.children().get(ColumnPath.of("id"));
        assertThat(restoredIds.getInt(0)).isEqualTo(100);
        assertThat(restoredIds.getInt(1)).isEqualTo(200);
        assertThat(restoredIds.getInt(2)).isEqualTo(300);
    }

    @Test
    void encodeListRejectsNonZeroBasedOffsets() {
        IntVector child = IntVector.materialized(new int[] {10, 20, 30}, Validity.allValid(3));
        // offsets must start at 0; a 1-based offsets array is not Arrow-compliant
        ListVector badList = new ListVector(new int[] {1, 3}, child, Validity.allValid(1), 1);
        assertThatThrownBy(() -> ArrowBufferCodec.encode(badList))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("zero-based");
    }

    @Test
    void encodeMapRejectsNonZeroBasedOffsets() {
        IntVector keys = IntVector.materialized(new int[] {1, 2}, Validity.allValid(2));
        IntVector values = IntVector.materialized(new int[] {10, 20}, Validity.allValid(2));
        MapVector badMap = new MapVector(new int[] {1, 2}, keys, values, Validity.allValid(1), 1);
        assertThatThrownBy(() -> ArrowBufferCodec.encode(badMap))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("zero-based");
    }

    private static Validity ageValidity() {
        BitSet valid = new BitSet(3);
        valid.set(0);
        valid.set(2); // age[1] null
        return Validity.of(valid, 3);
    }

    private static BinaryVector namesVector() {
        return binaryVector(utf8("ann"), utf8("bob"), utf8("cleo"));
    }

    private static BinaryVector mapValuesVector() {
        return binaryVector(utf8("x"), utf8("yy"), utf8("zzz"));
    }

    private static BinaryVector variantMetadataVector() {
        byte[] empty = {1, 0, 0};
        return binaryVector(empty, empty, empty);
    }

    private static BinaryVector variantValueVector() {
        return binaryVector(new byte[] {12}, new byte[] {0}, new byte[] {44});
    }

    private static BinaryVector binaryVector(byte[]... rows) {
        byte[] backing = new byte[totalLength(rows)];
        int[] offsets = new int[rows.length + 1];
        int cursor = 0;
        for (int i = 0; i < rows.length; i++) {
            System.arraycopy(rows[i], 0, backing, cursor, rows[i].length);
            cursor += rows[i].length;
            offsets[i + 1] = cursor;
        }
        return BinaryVector.of(heap(backing), IntSequence.of(offsets), Validity.allValid(rows.length));
    }

    private static int totalLength(byte[]... rows) {
        int total = 0;
        for (byte[] row : rows) {
            total += row.length;
        }
        return total;
    }

    private static byte[] bytesAt(ColumnVector vector, int row) {
        MemorySegment value = vector.get(row);
        return value.toArray(JAVA_BYTE);
    }

    private static MemorySegment heap(byte[] bytes) {
        return MemorySegment.ofArray(bytes);
    }

    private static byte[] utf8(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static ColumnVector roundTrip(ColumnVector vector, ColumnType type) {
        EncodedNode node = ArrowBufferCodec.encode(vector);
        return ArrowBufferCodec.decode(node, type);
    }
}
