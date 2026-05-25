/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.data;

import static io.tileverse.parquetry.filter.Pred.col;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Verifies that {@code read(predicate, ...)} returns only rows that satisfy the predicate, not merely the rows in the
 * row groups and pages that survive metadata pruning. The fixture packs four rows into a single row group whose
 * {@code pop} statistics span the threshold, so the metadata tiers cannot prune any of them; only record-level
 * evaluation yields the correct result.
 */
class RecordLevelFilterTest {

    private static final ColumnPath POP = ColumnPath.of("pop");
    private static final ColumnPath ID = ColumnPath.of("id");

    @TempDir
    Path tempDir;

    @Test
    void readAppliesTheRecordLevelPredicate() throws Exception {
        Path file = writeFixture();
        try (Storage storage = StorageFactory.open(file.getParent().toUri());
                RangeReader reader = storage.openRangeReader(file.getFileName().toString())) {
            ParquetDataset dataset = ParquetDataset.open(reader);
            try (Stream<ParquetRecord> rows =
                    dataset.read(col("pop").gt(1_000_000), Projection.ALL, ReadOptions.DEFAULTS)) {
                List<Integer> ids = rows.map(r -> r.getInt(ID)).sorted().toList();
                assertThat(ids).containsExactly(2, 4);
            }
        }
    }

    @Test
    void recordLevelPredicateUsesColumnsOutsideTheProjection() throws Exception {
        Path file = writeFixture();
        try (Storage storage = StorageFactory.open(file.getParent().toUri());
                RangeReader reader = storage.openRangeReader(file.getFileName().toString())) {
            ParquetDataset dataset = ParquetDataset.open(reader);
            Projection idOnly = Projection.of(Set.of(ID));
            try (Stream<ParquetRecord> rows = dataset.read(col("pop").gt(1_000_000), idOnly, ReadOptions.DEFAULTS)) {
                List<Integer> ids = rows.map(r -> r.getInt(ID)).sorted().toList();
                assertThat(ids).containsExactly(2, 4);
            }
        }
    }

    @Test
    void disablingTheRecordLevelFilterFallsBackToPushdownOnly() throws Exception {
        Path file = writeFixture();
        ReadOptions pushdownOnly =
                ReadOptions.builder().useRecordLevelFilter(false).build();
        try (Storage storage = StorageFactory.open(file.getParent().toUri());
                RangeReader reader = storage.openRangeReader(file.getFileName().toString())) {
            ParquetDataset dataset = ParquetDataset.open(reader);
            try (Stream<ParquetRecord> rows = dataset.read(col("pop").gt(1_000_000), Projection.ALL, pushdownOnly)) {
                assertThat(rows.count()).isEqualTo(4L);
            }
        }
    }

    private Path writeFixture() throws Exception {
        Path file = tempDir.resolve("record-filter.parquet");
        ParquetSchema schema = flatSchema(requiredInt32("pop"), requiredInt32("id"));
        WriteOptions options = WriteOptions.builder().tempDir(tempDir).build();
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(file), schema, options)) {
            writer.write(row(500_000, 1));
            writer.write(row(1_500_000, 2));
            writer.write(row(800_000, 3));
            writer.write(row(2_000_000, 4));
        }
        return file;
    }

    private static ParquetSchema flatSchema(SchemaNode.Primitive... leaves) {
        List<SchemaNode> children = Stream.of(leaves).map(f -> (SchemaNode) f).toList();
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, children, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive requiredInt32(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static WriteRow row(int pop, int id) {
        Map<ColumnPath, Object> values = Map.of(POP, pop, ID, id);
        return values::get;
    }
}
