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
package io.tileverse.parquetry.internal.read;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.LongVector;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.filter.RowRanges.Range;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.internal.filter.bloom.SplitBlockBloomFilter;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Verifies that the skip-decode mode of {@link BatchColumnReader} emits exactly the same rows as the default compacting
 * path while materializing only the selected non-null values.
 */
class SkipDecodeColumnReadTest {

    private static final ColumnPath V = ColumnPath.of("v");
    private static final int ROW_COUNT = 50;
    private static final int ROWS_PER_PAGE = 5;

    @TempDir
    Path tempDir;

    @Test
    void skipDecodeMatchesCompactPathForRequiredColumn() throws Exception {
        Path file = writeRequiredLongs();
        ParquetSchema schema = flatSchema(requiredInt64("v"));

        // Selected rows are spread across several of the ten pages.
        RowRanges mask = new RowRanges(List.of(new Range(4, 5), new Range(22, 22), new Range(47, 47)));
        int selectedNonNull = 4;

        DrainResult compact = drainWith(file, schema, mask, false);
        DrainResult skip = drainWith(file, schema, mask, true);

        assertThat(skip.values).as("emitted values match the compact path").isEqualTo(compact.values);
        assertThat(skip.nulls).as("emitted nulls match the compact path").isEqualTo(compact.nulls);
        assertThat(skip.values).hasSize((int) mask.totalRows());

        assertThat(skip.decodedValueCount)
                .as("skip-decode materializes only the selected non-null values")
                .isEqualTo(selectedNonNull)
                .as("skip-decode materializes far fewer values than the compact path")
                .isLessThan(compact.decodedValueCount);
    }

    @Test
    void skipDecodeMatchesCompactPathForNullableColumn() throws Exception {
        Path file = writeNullableLongs();
        ParquetSchema schema = flatSchema(optionalInt64("v"));

        // Mask covers null and non-null rows, selected and unselected alike.
        RowRanges mask = new RowRanges(List.of(new Range(3, 6), new Range(20, 23), new Range(48, 49)));
        int selectedNonNull = countSelectedNonNull(mask);

        DrainResult compact = drainWith(file, schema, mask, false);
        DrainResult skip = drainWith(file, schema, mask, true);

        assertThat(skip.values).as("emitted values match the compact path").isEqualTo(compact.values);
        assertThat(skip.nulls).as("emitted nulls match the compact path").isEqualTo(compact.nulls);
        assertThat(skip.values).hasSize((int) mask.totalRows());

        assertThat(skip.decodedValueCount)
                .as("skip-decode materializes only the selected non-null values")
                .isEqualTo(selectedNonNull)
                .as("skip-decode materializes far fewer values than the compact path")
                .isLessThan(compact.decodedValueCount);
    }

    private DrainResult drainWith(Path file, ParquetSchema schema, RowRanges mask, boolean skipDecode)
            throws Exception {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            FileMetaData footer = ParquetFormat.readFooter(source);
            RowGroup rowGroup = footer.rowGroups().get(0);
            OffsetIndex offsetIndex = loadOffsetIndex(source, rowGroup);

            IndexSectionLoader loader = new IndexSectionLoader() {
                @Override
                public OffsetIndex readOffsetIndex(long offset, int length) {
                    return ParquetFormat.readOffsetIndex(source, offset, length);
                }

                @Override
                public ColumnIndex readColumnIndex(long offset, int length) {
                    return ParquetFormat.readColumnIndex(source, offset, length);
                }

                @Override
                public SplitBlockBloomFilter readBloom(long offset, int length) {
                    throw new UnsupportedOperationException("bloom filters not used in this test");
                }
            };
            RowGroupChunks chunks = RowGroupChunks.of(rowGroup, schema, loader);
            RowGroupFetcher fetcher = TestFetchers.over(source, schema, schema, SegmentPool.getDefault());
            RowGroupSurvivor survivor = new RowGroupSurvivor(chunks, Optional.of(mask), true);
            try (RowGroupFetch fetch = fetcher.fetch(survivor, fetcher.planFor(survivor), BudgetReservation.NONE)) {
                FetchedColumnChunk chunk = fetch.columns().get(0);
                BatchColumnReader colReader = new BatchColumnReader(
                        TestDecodeBuffers.ample(), chunk, leaf(schema), mask, offsetIndex, skipDecode);

                DrainResult result = drainLongs(colReader);
                colReader.close();
                return result;
            }
        }
    }

    private static DrainResult drainLongs(BatchColumnReader colReader) {
        List<Long> values = new ArrayList<>();
        List<Boolean> nulls = new ArrayList<>();
        List<AutoCloseable> acquiredBuffers = new ArrayList<>();
        while (colReader.hasMore()) {
            int n = colReader.rowsRemainingInCurrentPage();
            ColumnVector vec = colReader.readBatch(n, acquiredBuffers);
            LongVector longs = (LongVector) vec;
            Validity validity = longs.validity();
            for (int i = 0; i < longs.size(); i++) {
                boolean isNull = !validity.isValid(i);
                nulls.add(isNull);
                values.add(isNull ? null : longs.getLong(i));
            }
        }
        return new DrainResult(values, nulls, colReader.decodedValueCount());
    }

    private static int countSelectedNonNull(RowRanges mask) {
        int count = 0;
        for (RowRanges.Range range : mask.ranges()) {
            for (long row = range.first(); row <= range.last(); row++) {
                if (!isNullRow((int) row)) {
                    count++;
                }
            }
        }
        return count;
    }

    /** Every third row is written as null in the nullable fixture. */
    private static boolean isNullRow(int row) {
        return row % 3 == 0;
    }

    private Path writeRequiredLongs() throws Exception {
        Path file = tempDir.resolve("skip-required.parquet");
        ParquetSchema schema = flatSchema(requiredInt64("v"));
        List<Map<ColumnPath, Object>> rows = new ArrayList<>();
        for (int v = 0; v < ROW_COUNT; v++) {
            rows.add(requiredRow(v));
        }
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, pageEvery())) {
            writer.writeBatch(WriteFixtures.batch(schema, rows));
        }
        return file;
    }

    private Path writeNullableLongs() throws Exception {
        Path file = tempDir.resolve("skip-nullable.parquet");
        ParquetSchema schema = flatSchema(optionalInt64("v"));
        List<Map<ColumnPath, Object>> rows = new ArrayList<>();
        for (int v = 0; v < ROW_COUNT; v++) {
            rows.add(nullableRow(v));
        }
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, pageEvery())) {
            writer.writeBatch(WriteFixtures.batch(schema, rows));
        }
        return file;
    }

    private WriteOptions pageEvery() {
        return WriteOptions.builder()
                .tempDir(tempDir)
                .pageValueLimit(ROWS_PER_PAGE)
                .build();
    }

    private static OffsetIndex loadOffsetIndex(ByteRangeSource source, RowGroup rowGroup) {
        ColumnChunk chunk = rowGroup.columns().get(0);
        return ParquetFormat.readOffsetIndex(
                source,
                chunk.offsetIndexOffset().getAsLong(),
                chunk.offsetIndexLength().getAsInt());
    }

    private static SchemaNode.Primitive leaf(ParquetSchema schema) {
        return (SchemaNode.Primitive) schema.find(V).orElseThrow();
    }

    private static ParquetSchema flatSchema(SchemaNode.Primitive... leaves) {
        List<SchemaNode> children = Stream.of(leaves).map(f -> (SchemaNode) f).toList();
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, children, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive requiredInt64(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive optionalInt64(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.OPTIONAL, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static Map<ColumnPath, Object> requiredRow(int v) {
        return Map.of(V, (long) v);
    }

    private static Map<ColumnPath, Object> nullableRow(int v) {
        Map<ColumnPath, Object> values = new HashMap<>();
        if (!isNullRow(v)) {
            values.put(V, (long) v);
        }
        return values;
    }

    private record DrainResult(List<Long> values, List<Boolean> nulls, long decodedValueCount) {}
}
