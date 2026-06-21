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
package io.tileverse.parquetry.internal.write;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ParquetReader;
import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.data.ParquetWriteException;
import io.tileverse.parquetry.data.ParquetWriter;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class UuidWriteRoundTripTest {

    @TempDir
    Path tempDir;

    @Test
    void writesAndReadsBackUuidColumnFromUuidValues() throws Exception {
        ParquetSchema schema = flatSchema(uuidColumn("id"));
        UUID a = UUID.fromString("0123456f-89ab-cdef-fedc-ba9876543210");
        UUID b = new UUID(0xFFFFFFFFFFFFFFFFL, 1L);
        Path file = tempDir.resolve("uuid.parquet");

        WriteOptions options = WriteOptions.builder().tempDir(tempDir).build();
        List<Map<ColumnPath, Object>> rowMaps = List.of(Map.of(ColumnPath.of("id"), a), Map.of(ColumnPath.of("id"), b));
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(file), schema, options)) {
            writer.writeBatch(WriteFixtures.batch(schema, rowMaps));
        }

        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetReader reader = ParquetReader.open(source);
            try (Stream<ParquetRecord> rows =
                    reader.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                assertThat(rows.map(r -> r.getUuid(ColumnPath.of("id")))).containsExactlyInAnyOrder(a, b);
            }
        }
    }

    @Test
    void rejectsValueAuthoredWithTheWrongTypeForAColumn() {
        ParquetSchema schema = flatSchema(byteArrayColumn("text"));
        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(schema);
        ColumnPath text = ColumnPath.of("text");

        assertThatThrownBy(() -> builder.setInt(text, 42))
                .isInstanceOf(ParquetWriteException.class)
                .hasMessageContaining("does not accept");
    }

    private static SchemaNode.Primitive byteArrayColumn(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive uuidColumn(String name) {
        return new SchemaNode.Primitive(
                name,
                Repetition.REQUIRED,
                PrimitiveKind.FIXED_LEN_BYTE_ARRAY,
                OptionalInt.of(16),
                Optional.of(new LogicalType.UuidType()),
                -1);
    }

    private static ParquetSchema flatSchema(SchemaNode.Primitive... leaves) {
        List<SchemaNode> children = Stream.of(leaves).map(f -> (SchemaNode) f).toList();
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, children, Optional.empty(), -1);
        return new ParquetSchema(root);
    }
}
