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

import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/** End-to-end: a page-selective predicate over a multi-page row group returns exactly the matching rows. */
class PagePruningReadTest {

    private static final ColumnPath V = ColumnPath.of("v");
    private static final ColumnPath TAG = ColumnPath.of("tag");

    @TempDir
    Path tempDir;

    @Test
    void readMatchesABruteForceFilterAcrossPages() throws Exception {
        Path file = writeOneHundredRows();
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetReader dataset = ParquetReader.open(source);
            try (Stream<ParquetRecord> rows = dataset.read(col("v").gtEq(80), Projection.ALL, ReadOptions.DEFAULTS)) {
                List<Integer> got = rows.map(r -> r.getInt(V)).sorted().toList();
                List<Integer> expected =
                        Stream.iterate(80, n -> n + 1).limit(20).toList();
                assertThat(got).isEqualTo(expected);
            }
        }
    }

    @Test
    void predicateOnAColumnOutsideTheProjectionStillPrunesPages() throws Exception {
        Path file = writeOneHundredRows();
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetReader dataset = ParquetReader.open(source);
            Projection tagOnly = Projection.of(Set.of(TAG));
            try (Stream<ParquetRecord> rows = dataset.read(col("v").gtEq(95), tagOnly, ReadOptions.DEFAULTS)) {
                List<Integer> tags = rows.map(r -> r.getInt(TAG)).sorted().toList();
                assertThat(tags).containsExactly(950, 960, 970, 980, 990);
            }
        }
    }

    @Test
    void disablingColumnIndexReproducesIdenticalRows() throws Exception {
        Path file = writeOneHundredRows();
        ReadOptions noColumnIndex =
                ReadOptions.builder().useColumnIndexFilter(false).build();
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetReader dataset = ParquetReader.open(source);
            try (Stream<ParquetRecord> rows = dataset.read(col("v").gtEq(80), Projection.ALL, noColumnIndex)) {
                List<Integer> got = rows.map(r -> r.getInt(V)).sorted().toList();
                assertThat(got)
                        .isEqualTo(Stream.iterate(80, n -> n + 1).limit(20).toList());
            }
        }
    }

    @Test
    void readWithoutNarrowingReturnsEveryMatchingRow() throws Exception {
        Path file = writeOneHundredRows();
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetReader dataset = ParquetReader.open(source);
            // A predicate every page can satisfy: the tier passes all pages, no mask, full decode.
            try (Stream<ParquetRecord> rows = dataset.read(col("v").gtEq(0), Projection.ALL, ReadOptions.DEFAULTS)) {
                assertThat(rows.count()).isEqualTo(100L);
            }
        }
    }

    private Path writeOneHundredRows() throws Exception {
        Path file = tempDir.resolve("page-prune-read.parquet");
        ParquetSchema schema = flatSchema(requiredInt32("v"), requiredInt32("tag"));
        WriteOptions options =
                WriteOptions.builder().tempDir(tempDir).pageValueLimit(8).build();
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(file), schema, options)) {
            for (int v = 0; v < 100; v++) {
                writer.write(row(v, v * 10));
            }
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

    private static WriteRow row(int v, int tag) {
        Map<ColumnPath, Object> values = Map.of(V, v, TAG, tag);
        return values::get;
    }
}
