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
package io.tileverse.parquetry.record;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.batch.BinaryVector;
import io.tileverse.parquetry.batch.BooleanVector;
import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.DefaultParquetRecordBatch;
import io.tileverse.parquetry.batch.DoubleVector;
import io.tileverse.parquetry.batch.FloatVector;
import io.tileverse.parquetry.batch.IntVector;
import io.tileverse.parquetry.batch.LongVector;
import io.tileverse.parquetry.batch.Validity;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.ParquetSchemaException;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Verifies the positional and {@link ColumnPath} typed accessors, the {@code read*}/{@code get*} split, and the
 * type-mismatch diagnostics {@link DefaultParquetRecord} exposes over a batch row.
 */
class DefaultParquetRecordTest {

    private static final ColumnPath BOOL_COL = ColumnPath.of("col_bool");
    private static final ColumnPath INT_COL = ColumnPath.of("col_int");
    private static final ColumnPath LONG_COL = ColumnPath.of("col_long");
    private static final ColumnPath FLOAT_COL = ColumnPath.of("col_float");
    private static final ColumnPath DOUBLE_COL = ColumnPath.of("col_double");
    private static final ColumnPath BINARY_COL = ColumnPath.of("col_binary");
    private static final ColumnPath NULL_COL = ColumnPath.of("col_null");

    private static final byte[] BINARY_PAYLOAD = "hello".getBytes(StandardCharsets.UTF_8);

    private ParquetSchema schema;
    private DefaultParquetRecordBatch batch;
    private ParquetRecord row;

    @BeforeEach
    void setUp() {
        schema = new ParquetSchema(new SchemaNode.Group(
                "root",
                Repetition.REQUIRED,
                List.of(
                        leaf(BOOL_COL.name(), PrimitiveKind.BOOLEAN),
                        leaf(INT_COL.name(), PrimitiveKind.INT32),
                        leaf(LONG_COL.name(), PrimitiveKind.INT64),
                        leaf(FLOAT_COL.name(), PrimitiveKind.FLOAT),
                        leaf(DOUBLE_COL.name(), PrimitiveKind.DOUBLE),
                        leaf(BINARY_COL.name(), PrimitiveKind.BYTE_ARRAY),
                        leaf(NULL_COL.name(), PrimitiveKind.INT32)),
                Optional.empty(),
                -1));

        Validity present = Validity.allValid(1);
        Map<ColumnPath, ColumnVector> columns = new LinkedHashMap<>();
        columns.put(BOOL_COL, BooleanVector.materialized(new boolean[] {true}, present));
        columns.put(INT_COL, IntVector.materialized(new int[] {42}, present));
        columns.put(LONG_COL, LongVector.materialized(new long[] {7_000_000_000L}, present));
        columns.put(FLOAT_COL, FloatVector.materialized(new float[] {1.5f}, present));
        columns.put(DOUBLE_COL, DoubleVector.materialized(new double[] {3.25}, present));
        columns.put(
                BINARY_COL,
                BinaryVector.materialized(
                        new MemorySegment[] {
                            MemorySegment.ofArray(BINARY_PAYLOAD).asReadOnly()
                        },
                        present));
        columns.put(NULL_COL, IntVector.materialized(new int[] {0}, Validity.of(new BitSet(), 1)));

        batch = new DefaultParquetRecordBatch(schema, columns, 1, Arena.ofConfined());
        row = batch.materialize(0);
    }

    @AfterEach
    void tearDown() {
        batch.close();
    }

    @Test
    void schemaReturnsTheProjectedSchemaPassedIn() {
        assertThat(row.schema()).isSameAs(schema);
    }

    @Test
    void positionalTypedAccessorsReadStraightFromTheVector() {
        assertThat(row.columnCount()).isEqualTo(7);
        assertThat(row.columnPath(1)).isEqualTo(INT_COL);
        assertThat(row.getBoolean(0)).isTrue();
        assertThat(row.getInt(1)).isEqualTo(42);
        assertThat(row.getLong(2)).isEqualTo(7_000_000_000L);
        assertThat(row.getFloat(3)).isEqualTo(1.5f);
        assertThat(row.getDouble(4)).isEqualTo(3.25);
    }

    @Test
    void columnPathTypedAccessorsResolveToTheSamePosition() {
        assertThat(row.getBoolean(BOOL_COL)).isTrue();
        assertThat(row.getInt(INT_COL)).isEqualTo(42);
        assertThat(row.getLong(LONG_COL)).isEqualTo(7_000_000_000L);
        assertThat(row.getFloat(FLOAT_COL)).isEqualTo(1.5f);
        assertThat(row.getDouble(DOUBLE_COL)).isEqualTo(3.25);
    }

    @Test
    void getReturnsTheBoxedValueOrMemorySegment() {
        assertThat(row.get(INT_COL)).isEqualTo(42);
        assertThat(row.get(BINARY_COL)).isInstanceOf(MemorySegment.class);
    }

    @Test
    void getBinaryMaterializesAFreshByteArrayEachCall() {
        byte[] first = row.getBinary(BINARY_COL);
        byte[] second = row.getBinary(BINARY_COL);

        assertThat(first).containsExactly(BINARY_PAYLOAD);
        assertThat(second).containsExactly(BINARY_PAYLOAD);
        assertThat(first).isNotSameAs(second);
    }

    @Test
    void getStringUtf8DecodesTheBinaryBytes() {
        assertThat(row.getString(BINARY_COL)).isEqualTo("hello");
    }

    @Test
    void readBinaryHandsTheBackingToTheViewWithoutCopying() {
        long viewedLength = row.readBinary(BINARY_COL, (backing, offset, length) -> length);
        assertThat(viewedLength).isEqualTo(BINARY_PAYLOAD.length);

        String viewed = row.readBinary(
                BINARY_COL,
                (backing, offset, length) -> new String(
                        backing.asSlice(offset, length).toArray(java.lang.foreign.ValueLayout.JAVA_BYTE),
                        StandardCharsets.UTF_8));
        assertThat(viewed).isEqualTo("hello");
    }

    @Test
    void isNullReportsTrueForNullLeafValue() {
        assertThat(row.isNull(NULL_COL)).isTrue();
        assertThat(row.isNull(INT_COL)).isFalse();
        assertThat(row.getBinary(NULL_COL))
                .as("binary getters on a null cell return null")
                .isNull();
    }

    @Test
    void getIntOnLongColumnThrowsWithDiagnosticMessage() {
        assertThatThrownBy(() -> row.getInt(LONG_COL))
                .isInstanceOf(ParquetSchemaException.class)
                .hasMessageContaining(LONG_COL.dot())
                .hasMessageContaining("INT64")
                .hasMessageContaining("getInt");
    }

    @Test
    void getStringOnIntColumnThrowsWithDiagnosticMessage() {
        assertThatThrownBy(() -> row.getString(INT_COL))
                .isInstanceOf(ParquetSchemaException.class)
                .hasMessageContaining(INT_COL.dot())
                .hasMessageContaining("INT32")
                .hasMessageContaining("getString");
    }

    @Test
    void getIntOnUnknownColumnThrowsDiagnosticMessage() {
        ColumnPath missing = ColumnPath.of("does_not_exist");
        assertThatThrownBy(() -> row.getInt(missing))
                .isInstanceOf(ParquetSchemaException.class)
                .hasMessageContaining("does_not_exist")
                .hasMessageContaining("not present in the projected schema")
                .hasMessageContaining("getInt");
    }

    @Test
    void detachCopiesValuesAndStaysValidPastTheBatch() {
        ParquetRecord detached = row.detach();
        batch.close(); // close the source batch; the detached copy must still read

        assertThat(detached).isInstanceOf(DetachedParquetRecord.class);
        assertThat(detached.getInt(INT_COL)).isEqualTo(42);
        assertThat(detached.getDouble(DOUBLE_COL)).isEqualTo(3.25);
        assertThat(detached.getString(BINARY_COL)).isEqualTo("hello");
        assertThat(detached.isNull(NULL_COL)).isTrue();
        assertThat(detached.detach()).as("detach is idempotent").isSameAs(detached);
    }

    private static SchemaNode.Primitive leaf(String name, PrimitiveKind kind) {
        return new SchemaNode.Primitive(name, Repetition.OPTIONAL, kind, OptionalInt.empty(), Optional.empty(), -1);
    }
}
