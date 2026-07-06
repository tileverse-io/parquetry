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
package io.tileverse.parquetry;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class UuidColumnRoundTripIT {

    private static final ColumnPath ID = ColumnPath.of("id");

    @TempDir
    Path tempDir;

    @Test
    void eqInAndRangePredicatesReturnExactRowsAcrossRowGroups() throws Exception {
        // Deterministic UUIDs with ascending high bits 0..999. All high bits are positive, which means unsigned and
        // signed order agree here; that keeps the range assertion simple. The unsigned-vs-signed divergence is pinned
        // by the unit tests (UuidConverterTest / UuidValueComparisonTest). Across 10 row groups of 100, the row-group
        // stats
        // ranges do not overlap, which lets the STATS tier prune.
        List<UUID> all = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            all.add(new UUID(i, 0L));
        }
        Path file = tempDir.resolve("uuids.parquet");
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .rowGroupSize(WriteOptions.RowGroupSize.rows(100))
                .build();
        ParquetSchema schema = flatSchema(uuidColumn("id"));
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, options)) {
            ParquetRecordBatchBuilder appender = writer.appender(100);
            for (UUID u : all) {
                WriteFixtures.appendRow(appender, schema, Map.of(ID, u));
            }
            appender.flush();
        }

        UUID target = all.get(523);
        assertThat(readIds(file, Pred.col(ID).eq(target))).containsExactly(target);

        UUID t1 = all.get(100);
        UUID t2 = all.get(900);
        assertThat(readIds(file, Pred.col(ID).inUuids(t1, t2))).containsExactlyInAnyOrder(t1, t2);

        List<UUID> firstFive = all.subList(0, 5);
        assertThat(readIds(file, Pred.col(ID).lt(all.get(5)))).containsExactlyInAnyOrderElementsOf(firstFive);
    }

    private List<UUID> readIds(Path file, Predicate predicate) throws Exception {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            try (Stream<ParquetRecord> rows = reader.read(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
                return rows.map(r -> r.getUuid(ID)).toList();
            }
        }
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
