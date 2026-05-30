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

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.materializer.RowAccessor;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.ParquetSchemaException;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Verifies the typed accessors, lazy binary materialization, and type-mismatch diagnostics that
 * {@link DefaultParquetRecord} exposes on top of a {@link RowAccessor}.
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
    private MapRowAccessor row;
    private DefaultParquetRecord parquetRecord;

    @BeforeEach
    void setUp() {
        schema = new ParquetSchema(new SchemaNode.Group(
                "root",
                Repetition.REQUIRED,
                List.of(
                        leaf(BOOL_COL.parts().get(0), PrimitiveKind.BOOLEAN),
                        leaf(INT_COL.parts().get(0), PrimitiveKind.INT32),
                        leaf(LONG_COL.parts().get(0), PrimitiveKind.INT64),
                        leaf(FLOAT_COL.parts().get(0), PrimitiveKind.FLOAT),
                        leaf(DOUBLE_COL.parts().get(0), PrimitiveKind.DOUBLE),
                        leaf(BINARY_COL.parts().get(0), PrimitiveKind.BYTE_ARRAY),
                        leaf(NULL_COL.parts().get(0), PrimitiveKind.INT32)),
                Optional.empty(),
                -1));

        row = new MapRowAccessor();
        row.put(BOOL_COL, Boolean.TRUE);
        row.put(INT_COL, Integer.valueOf(42));
        row.put(LONG_COL, Long.valueOf(7_000_000_000L));
        row.put(FLOAT_COL, Float.valueOf(1.5f));
        row.put(DOUBLE_COL, Double.valueOf(3.25));
        row.put(BINARY_COL, MemorySegment.ofArray(BINARY_PAYLOAD).asReadOnly());
        row.put(NULL_COL, null);

        parquetRecord = new DefaultParquetRecord(schema, row);
    }

    @Test
    void schemaReturnsTheProjectedSchemaPassedIn() {
        assertThat(parquetRecord.schema()).isSameAs(schema);
    }

    @Test
    void typedAccessorsReturnPrimitiveValues() {
        assertThat(parquetRecord.getBoolean(BOOL_COL)).isTrue();
        assertThat(parquetRecord.getInt(INT_COL)).isEqualTo(42);
        assertThat(parquetRecord.getLong(LONG_COL)).isEqualTo(7_000_000_000L);
        assertThat(parquetRecord.getFloat(FLOAT_COL)).isEqualTo(1.5f);
        assertThat(parquetRecord.getDouble(DOUBLE_COL)).isEqualTo(3.25);
    }

    @Test
    void getReturnsTheRawBoxedValueOrMemorySegment() {
        assertThat(parquetRecord.get(INT_COL)).isEqualTo(42);
        Object raw = parquetRecord.get(BINARY_COL);
        assertThat(raw).isInstanceOf(MemorySegment.class);
    }

    @Test
    void getBinaryMaterializesAFreshByteArrayEachCall() {
        byte[] first = parquetRecord.getBinary(BINARY_COL);
        byte[] second = parquetRecord.getBinary(BINARY_COL);

        assertThat(first).containsExactly(BINARY_PAYLOAD);
        assertThat(second).containsExactly(BINARY_PAYLOAD);
        assertThat(first).isNotSameAs(second);
    }

    @Test
    void getStringUtf8DecodesTheBinaryBytes() {
        assertThat(parquetRecord.getString(BINARY_COL)).isEqualTo("hello");
    }

    @Test
    void isNullReportsTrueForNullLeafValue() {
        assertThat(parquetRecord.isNull(NULL_COL)).isTrue();
        assertThat(parquetRecord.isNull(INT_COL)).isFalse();
    }

    @Test
    void isNullReportsTrueForNullGroup() {
        ColumnPath groupPath = ColumnPath.of("missing_group");
        row.markGroupNull(groupPath);
        assertThat(parquetRecord.isNull(groupPath)).isTrue();
    }

    @Test
    void getIntOnLongColumnThrowsWithDiagnosticMessage() {
        assertThatThrownBy(() -> parquetRecord.getInt(LONG_COL))
                .isInstanceOf(ParquetSchemaException.class)
                .hasMessageContaining(LONG_COL.dot())
                .hasMessageContaining("INT64")
                .hasMessageContaining("getInt");
    }

    @Test
    void getStringOnIntColumnThrowsWithDiagnosticMessage() {
        assertThatThrownBy(() -> parquetRecord.getString(INT_COL))
                .isInstanceOf(ParquetSchemaException.class)
                .hasMessageContaining(INT_COL.dot())
                .hasMessageContaining("INT32")
                .hasMessageContaining("getString");
    }

    @Test
    void getListAndGetStructThrowOnNonNestedColumns() {
        assertThatThrownBy(() -> parquetRecord.getList(BINARY_COL))
                .as("getList on a primitive column")
                .isInstanceOf(ParquetSchemaException.class)
                .hasMessageContaining(BINARY_COL.dot())
                .hasMessageContaining("not a list column");
        assertThatThrownBy(() -> parquetRecord.getStruct(BINARY_COL))
                .as("getStruct on a primitive column")
                .isInstanceOf(ParquetSchemaException.class)
                .hasMessageContaining(BINARY_COL.dot())
                .hasMessageContaining("not a struct column");
    }

    @Test
    void getListAndGetStructReturnNullForNullCells() {
        assertThat(parquetRecord.getList(NULL_COL)).as("getList on a null cell").isNull();
        assertThat(parquetRecord.getStruct(NULL_COL))
                .as("getStruct on a null cell")
                .isNull();
    }

    @Test
    void getIntOnUnknownColumnThrowsDiagnosticMessage() {
        ColumnPath missing = ColumnPath.of("does_not_exist");
        assertThatThrownBy(() -> parquetRecord.getInt(missing))
                .isInstanceOf(ParquetSchemaException.class)
                .hasMessageContaining("does_not_exist")
                .hasMessageContaining("not present in the projected schema")
                .hasMessageContaining("getInt");
    }

    @Test
    void getIntOnNullColumnThrowsNullPointer() {
        assertThatThrownBy(() -> parquetRecord.getInt(NULL_COL)).isInstanceOf(NullPointerException.class);
    }

    private static SchemaNode.Primitive leaf(String name, PrimitiveKind kind) {
        return new SchemaNode.Primitive(name, Repetition.OPTIONAL, kind, OptionalInt.empty(), Optional.empty(), -1);
    }

    /** Test double: a {@link RowAccessor} backed by a mutable map. */
    private static final class MapRowAccessor implements RowAccessor {
        private final Map<ColumnPath, Object> values = new HashMap<>();
        private final Set<ColumnPath> nullGroups = new HashSet<>();

        void put(ColumnPath path, Object value) {
            values.put(path, value);
        }

        void markGroupNull(ColumnPath path) {
            nullGroups.add(path);
        }

        @Override
        public Object get(ColumnPath path) {
            return values.get(path);
        }

        @Override
        public boolean isGroupNull(ColumnPath path) {
            return nullGroups.contains(path);
        }

        @Override
        public Map<ColumnPath, Object> values() {
            return Map.copyOf(values);
        }
    }
}
