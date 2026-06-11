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
package io.tileverse.parquetry.internal.batch;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.batch.BinaryVector;
import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.DefaultParquetRecordBatch;
import io.tileverse.parquetry.batch.IntSequence;
import io.tileverse.parquetry.batch.IntVector;
import io.tileverse.parquetry.batch.ListVector;
import io.tileverse.parquetry.batch.MapVector;
import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.batch.StructVector;
import io.tileverse.parquetry.batch.Validity;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class BatchArrowLayoutTest {

    @Test
    void mixedColumnsRoundTripThroughEncodedBatch() {
        ParquetSchema schema = mixedSchema();
        ParquetRecordBatch original = mixedBatch(schema);

        EncodedBatch encoded = BatchArrowLayout.encode(original);
        ParquetRecordBatch restored = BatchArrowLayout.decode(encoded, schema);

        assertThat(restored.rowCount()).isEqualTo(original.rowCount());

        IntVector restoredId = (IntVector) restored.columns().get(ColumnPath.of("id"));
        assertThat(restoredId.getInt(0)).isEqualTo(10);
        assertThat(restoredId.isNull(1)).isTrue();
        assertThat(restoredId.getInt(2)).isEqualTo(30);

        BinaryVector restoredName = (BinaryVector) restored.columns().get(ColumnPath.of("name"));
        assertThat(bytesAt(restoredName, 0)).containsExactly(utf8("ann"));
        assertThat(bytesAt(restoredName, 1)).containsExactly(utf8("bob"));
        assertThat(bytesAt(restoredName, 2)).containsExactly(utf8("cleo"));

        ListVector restoredScores = (ListVector) restored.columns().get(ColumnPath.of("scores"));
        assertThat(restoredScores.size()).isEqualTo(3);
        assertThat(restoredScores.offsets()).containsExactly(0, 2, 2, 3);
        assertThat(restoredScores.validity().isNull(1)).isTrue();
        IntVector restoredScoreValues = (IntVector) restoredScores.child();
        assertThat(restoredScoreValues.getInt(0)).isEqualTo(100);
        assertThat(restoredScoreValues.getInt(1)).isEqualTo(200);
        assertThat(restoredScoreValues.getInt(2)).isEqualTo(300);

        original.close();
        restored.close();
    }

    @Test
    void structColumnRoundTripsThroughEncodedBatch() {
        ParquetSchema schema = structSchema();
        ParquetRecordBatch original = structBatch(schema);

        EncodedBatch encoded = BatchArrowLayout.encode(original);
        ParquetRecordBatch restored = BatchArrowLayout.decode(encoded, schema);

        assertThat(restored.rowCount()).isEqualTo(3);

        StructVector restoredPerson = (StructVector) restored.columns().get(ColumnPath.of("person"));
        assertThat(restoredPerson.size()).isEqualTo(3);

        IntVector restoredAges = (IntVector) restoredPerson.children().get(ColumnPath.of("age"));
        assertThat(restoredAges.getInt(0)).isEqualTo(7);
        assertThat(restoredAges.isNull(1)).isTrue();
        assertThat(restoredAges.getInt(2)).isEqualTo(9);

        BinaryVector restoredNames = (BinaryVector) restoredPerson.children().get(ColumnPath.of("name"));
        assertThat(bytesAt(restoredNames, 0)).containsExactly(utf8("ann"));
        assertThat(bytesAt(restoredNames, 1)).containsExactly(utf8("bob"));
        assertThat(bytesAt(restoredNames, 2)).containsExactly(utf8("cleo"));

        original.close();
        restored.close();
    }

    @Test
    void mapColumnRoundTripsThroughEncodedBatch() {
        ParquetSchema schema = mapSchema();
        ParquetRecordBatch original = mapBatch(schema);

        EncodedBatch encoded = BatchArrowLayout.encode(original);
        ParquetRecordBatch restored = BatchArrowLayout.decode(encoded, schema);

        assertThat(restored.rowCount()).isEqualTo(3);

        MapVector restoredTags = (MapVector) restored.columns().get(ColumnPath.of("tags"));
        assertThat(restoredTags.size()).isEqualTo(3);
        assertThat(restoredTags.offsets()).containsExactly(0, 2, 2, 3);

        IntVector restoredKeys = (IntVector) restoredTags.keys();
        assertThat(restoredKeys.getInt(0)).isEqualTo(1);
        assertThat(restoredKeys.getInt(1)).isEqualTo(2);
        assertThat(restoredKeys.getInt(2)).isEqualTo(3);

        BinaryVector restoredValues = (BinaryVector) restoredTags.values();
        assertThat(bytesAt(restoredValues, 0)).containsExactly(utf8("x"));
        assertThat(bytesAt(restoredValues, 1)).containsExactly(utf8("yy"));
        assertThat(bytesAt(restoredValues, 2)).containsExactly(utf8("zzz"));

        original.close();
        restored.close();
    }

    @Test
    void encodeListsColumnsInSchemaTopLevelOrder() {
        ParquetSchema schema = mixedSchema();
        ParquetRecordBatch original = mixedBatch(schema);

        EncodedBatch encoded = BatchArrowLayout.encode(original);

        assertThat(encoded.columns())
                .containsExactly(ColumnPath.of("id"), ColumnPath.of("name"), ColumnPath.of("scores"));
        assertThat(encoded.rowCount()).isEqualTo(3);
        assertThat(encoded.nodes()).hasSize(3);

        original.close();
    }

    private static ParquetRecordBatch mixedBatch(ParquetSchema schema) {
        Map<ColumnPath, ColumnVector> columns = new LinkedHashMap<>();
        columns.put(ColumnPath.of("id"), idVector());
        columns.put(ColumnPath.of("name"), nameVector());
        columns.put(ColumnPath.of("scores"), scoresVector());
        return new DefaultParquetRecordBatch(schema, columns, 3, Arena.ofConfined());
    }

    private static IntVector idVector() {
        BitSet valid = new BitSet(3);
        valid.set(0);
        valid.set(2); // id[1] null
        return IntVector.materialized(new int[] {10, 0, 30}, Validity.of(valid, 3));
    }

    private static BinaryVector nameVector() {
        return binaryVector(utf8("ann"), utf8("bob"), utf8("cleo"));
    }

    /** Rows: [100, 200], null, [300]. Child holds the three present values. */
    private static ListVector scoresVector() {
        IntVector child = IntVector.materialized(new int[] {100, 200, 300}, Validity.allValid(3));
        int[] offsets = {0, 2, 2, 3};
        BitSet valid = new BitSet(3);
        valid.set(0);
        valid.set(2); // scores[1] null
        return new ListVector(offsets, child, Validity.of(valid, 3), 3);
    }

    /**
     * {@code id} (int32 leaf), {@code name} (UTF-8 binary leaf), and {@code scores} (optional LIST of int32). The list
     * follows the standard three-level encoding: an optional LIST group wrapping a repeated group with one optional
     * int32 element.
     */
    private static ParquetSchema mixedSchema() {
        SchemaNode id = new SchemaNode.Primitive(
                "id", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode name = new SchemaNode.Primitive(
                "name",
                Repetition.OPTIONAL,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new io.tileverse.parquetry.format.LogicalType.StringType()),
                -1);
        SchemaNode.Group root = new SchemaNode.Group(
                "root", Repetition.REQUIRED, List.of(id, name, scoresGroup()), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Group scoresGroup() {
        SchemaNode element = new SchemaNode.Primitive(
                "element", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group repeated =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        return new SchemaNode.Group(
                "scores",
                Repetition.OPTIONAL,
                List.of(repeated),
                Optional.of(new io.tileverse.parquetry.format.LogicalType.ListType()),
                -1);
    }

    private static ParquetRecordBatch structBatch(ParquetSchema schema) {
        Map<ColumnPath, ColumnVector> columns = new LinkedHashMap<>();
        columns.put(ColumnPath.of("person"), personVector());
        return new DefaultParquetRecordBatch(schema, columns, 3, Arena.ofConfined());
    }

    /** A {@code person} struct with an int32 {@code age} (row 1 null) and a UTF-8 {@code name}. */
    private static StructVector personVector() {
        BitSet validAges = new BitSet(3);
        validAges.set(0);
        validAges.set(2); // age[1] null
        IntVector ages = IntVector.materialized(new int[] {7, 0, 9}, Validity.of(validAges, 3));
        BinaryVector names = binaryVector(utf8("ann"), utf8("bob"), utf8("cleo"));
        Map<ColumnPath, ColumnVector> children = new LinkedHashMap<>();
        children.put(ColumnPath.of("age"), ages);
        children.put(ColumnPath.of("name"), names);
        return new StructVector(children, Validity.allValid(3), 3);
    }

    /**
     * A single {@code person} struct column. A non-repeated group with no logical-type annotation classifies as STRUCT,
     * whose two leaf children resolve to the struct field types.
     */
    private static ParquetSchema structSchema() {
        SchemaNode age = new SchemaNode.Primitive(
                "age", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode name = new SchemaNode.Primitive(
                "name",
                Repetition.OPTIONAL,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new io.tileverse.parquetry.format.LogicalType.StringType()),
                -1);
        SchemaNode.Group person =
                new SchemaNode.Group("person", Repetition.OPTIONAL, List.of(age, name), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(person), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static ParquetRecordBatch mapBatch(ParquetSchema schema) {
        Map<ColumnPath, ColumnVector> columns = new LinkedHashMap<>();
        columns.put(ColumnPath.of("tags"), tagsVector());
        return new DefaultParquetRecordBatch(schema, columns, 3, Arena.ofConfined());
    }

    /** Rows: {1 -> "x", 2 -> "yy"}, {}, {3 -> "zzz"}. Flattened keys/values hold the three entries. */
    private static MapVector tagsVector() {
        IntVector keys = IntVector.materialized(new int[] {1, 2, 3}, Validity.allValid(3));
        BinaryVector values = binaryVector(utf8("x"), utf8("yy"), utf8("zzz"));
        int[] offsets = {0, 2, 2, 3};
        return new MapVector(offsets, keys, values, Validity.allValid(3), 3);
    }

    /**
     * A single {@code tags} map column. The MAP logical type plus a repeated {@code key_value} group holding a
     * {@code key} and a {@code value} child classifies as MAP, whose key/value leaves resolve to the entry types.
     */
    private static ParquetSchema mapSchema() {
        SchemaNode key = new SchemaNode.Primitive(
                "key", Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode value = new SchemaNode.Primitive(
                "value",
                Repetition.OPTIONAL,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new io.tileverse.parquetry.format.LogicalType.StringType()),
                -1);
        SchemaNode.Group keyValue =
                new SchemaNode.Group("key_value", Repetition.REPEATED, List.of(key, value), Optional.empty(), -1);
        SchemaNode.Group tags = new SchemaNode.Group(
                "tags",
                Repetition.OPTIONAL,
                List.of(keyValue),
                Optional.of(new io.tileverse.parquetry.format.LogicalType.MapType()),
                -1);
        SchemaNode.Group root = new SchemaNode.Group("root", Repetition.REQUIRED, List.of(tags), Optional.empty(), -1);
        return new ParquetSchema(root);
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
        return BinaryVector.of(MemorySegment.ofArray(backing), IntSequence.of(offsets), Validity.allValid(rows.length));
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

    private static byte[] utf8(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }
}
