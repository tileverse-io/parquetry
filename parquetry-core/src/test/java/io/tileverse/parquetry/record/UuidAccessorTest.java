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
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DefaultParquetRecordBatch;
import io.tileverse.parquetry.columnar.FixedLenBinaryVector;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.data.UuidConverter;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.ParquetSchemaException;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Verifies {@link ParquetRecord#getUuid(ColumnPath)} decodes a 16-byte big-endian FIXED_LEN_BYTE_ARRAY column to a
 * {@link UUID}, that {@link ParquetRecord#get(ColumnPath)} stays raw-physical (a {@link MemorySegment}), and that a
 * null cell reads back as a {@code null} UUID.
 */
class UuidAccessorTest {

    private static final ColumnPath ID_COL = ColumnPath.of("id");

    @Test
    void getUuidDecodesFixedLenBinaryColumnAndGetStaysPhysical() {
        UUID uuid = UUID.fromString("0123456f-89ab-cdef-fedc-ba9876543210");
        try (DefaultParquetRecordBatch batch = uuidBatch(uuid, Validity.allValid(1))) {
            ParquetRecord row = batch.materialize(0);

            assertThat(row.getUuid(ID_COL)).isEqualTo(uuid);
            assertThat(row.getUuid(0)).isEqualTo(uuid);
            assertThat(row.get(ID_COL)).isInstanceOf(MemorySegment.class);
        }
    }

    @Test
    void getUuidReturnsNullForNullCell() {
        UUID uuid = UUID.fromString("0123456f-89ab-cdef-fedc-ba9876543210");
        try (DefaultParquetRecordBatch batch = uuidBatch(uuid, Validity.of(new BitSet(), 1))) {
            ParquetRecord row = batch.materialize(0);

            assertThat(row.getUuid(ID_COL)).isNull();
            assertThat(row.getUuid(0)).isNull();
        }
    }

    @Test
    void detachedRecordDecodesUuidFromItsOwnedSegment() {
        UUID uuid = UUID.fromString("0123456f-89ab-cdef-fedc-ba9876543210");
        ParquetRecord detached;
        try (DefaultParquetRecordBatch batch = uuidBatch(uuid, Validity.allValid(1))) {
            detached = batch.materialize(0).detach();
        }

        assertThat(detached).isInstanceOf(DetachedParquetRecord.class);
        assertThat(detached.getUuid(ID_COL)).isEqualTo(uuid);
        assertThat(detached.getUuid(0)).isEqualTo(uuid);
    }

    @Test
    void detachedRecordReturnsNullUuidForNullCell() {
        UUID uuid = UUID.fromString("0123456f-89ab-cdef-fedc-ba9876543210");
        ParquetRecord detached;
        try (DefaultParquetRecordBatch batch = uuidBatch(uuid, Validity.of(new BitSet(), 1))) {
            detached = batch.materialize(0).detach();
        }

        assertThat(detached).isInstanceOf(DetachedParquetRecord.class);
        assertThat(detached.getUuid(ID_COL)).isNull();
        assertThat(detached.getUuid(0)).isNull();
    }

    @Test
    void getUuidThrowsForByteArrayColumn() {
        try (DefaultParquetRecordBatch batch = byteArrayBatch()) {
            ParquetRecord row = batch.materialize(0);

            assertThatThrownBy(() -> row.getUuid(ID_COL)).isInstanceOf(ParquetSchemaException.class);
            assertThatThrownBy(() -> row.getUuid(0)).isInstanceOf(ParquetSchemaException.class);
        }
    }

    @Test
    void getUuidThrowsForWrongWidthFixedLenBinaryColumn() {
        try (DefaultParquetRecordBatch batch = fixedLenBatch(12)) {
            ParquetRecord row = batch.materialize(0);

            assertThatThrownBy(() -> row.getUuid(ID_COL)).isInstanceOf(ParquetSchemaException.class);
            assertThatThrownBy(() -> row.getUuid(0)).isInstanceOf(ParquetSchemaException.class);
        }
    }

    @Test
    void detachedRecordThrowsForNon16ByteSegment() {
        ParquetRecord detached;
        try (DefaultParquetRecordBatch batch = byteArrayBatch()) {
            detached = batch.materialize(0).detach();
        }

        assertThat(detached).isInstanceOf(DetachedParquetRecord.class);
        assertThatThrownBy(() -> detached.getUuid(ID_COL)).isInstanceOf(ParquetSchemaException.class);
        assertThatThrownBy(() -> detached.getUuid(0)).isInstanceOf(ParquetSchemaException.class);
    }

    private static DefaultParquetRecordBatch byteArrayBatch() {
        ParquetSchema schema = singleColumnSchema(PrimitiveKind.BYTE_ARRAY, OptionalInt.empty());
        MemorySegment value = MemorySegment.ofArray(new byte[] {1, 2, 3, 4}).asReadOnly();
        BinaryVector vector = BinaryVector.materialized(new MemorySegment[] {value}, Validity.allValid(1));
        return batchOf(schema, vector);
    }

    private static DefaultParquetRecordBatch fixedLenBatch(int byteWidth) {
        ParquetSchema schema = singleColumnSchema(PrimitiveKind.FIXED_LEN_BYTE_ARRAY, OptionalInt.of(byteWidth));
        MemorySegment value = MemorySegment.ofArray(new byte[byteWidth]).asReadOnly();
        FixedLenBinaryVector vector =
                FixedLenBinaryVector.materialized(new MemorySegment[] {value}, byteWidth, Validity.allValid(1));
        return batchOf(schema, vector);
    }

    private static DefaultParquetRecordBatch batchOf(ParquetSchema schema, ColumnVector vector) {
        Map<ColumnPath, ColumnVector> columns = new LinkedHashMap<>();
        columns.put(ID_COL, vector);
        return new DefaultParquetRecordBatch(schema, columns, 1, Arena.ofConfined());
    }

    private static ParquetSchema singleColumnSchema(PrimitiveKind kind, OptionalInt typeLength) {
        return new ParquetSchema(new SchemaNode.Group(
                "root",
                Repetition.REQUIRED,
                List.of(new SchemaNode.Primitive(
                        ID_COL.name(), Repetition.OPTIONAL, kind, typeLength, Optional.empty(), -1)),
                Optional.empty(),
                -1));
    }

    private static DefaultParquetRecordBatch uuidBatch(UUID uuid, Validity validity) {
        ParquetSchema schema =
                singleColumnSchema(PrimitiveKind.FIXED_LEN_BYTE_ARRAY, OptionalInt.of(UuidConverter.BYTES));
        MemorySegment value = MemorySegment.ofArray(UuidConverter.toBytes(uuid)).asReadOnly();
        FixedLenBinaryVector vector =
                FixedLenBinaryVector.materialized(new MemorySegment[] {value}, UuidConverter.BYTES, validity);
        return batchOf(schema, vector);
    }
}
