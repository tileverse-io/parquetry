/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.columnar.LongVector;
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
 * A windowed masked read of one column emits exactly the rows the compacting masked read emits, over the same row mask,
 * while materializing only the selected non-null values.
 */
class MaskedPageSkipParityTest {

    private static final ColumnPath V = ColumnPath.of("v");
    private static final int ROW_COUNT = 50;
    private static final int ROWS_PER_PAGE = 5;

    @TempDir
    Path tempDir;

    @Test
    void windowedDecodeMatchesCompactPathForRequiredColumn() throws Exception {
        Path file = writeRequiredLongs();
        ParquetSchema schema = flatSchema(requiredInt64("v"));

        // Selected rows are spread across several of the ten pages.
        RowRanges mask = new RowRanges(List.of(new Range(4, 5), new Range(22, 22), new Range(47, 47)));
        int selectedNonNull = 4;

        DrainResult compact = drainCompacting(file, schema, mask);
        DrainResult windowed = drainWindowed(file, schema, mask);

        assertThat(windowed.values).as("emitted values match the compact path").isEqualTo(compact.values);
        assertThat(windowed.nulls).as("emitted nulls match the compact path").isEqualTo(compact.nulls);
        assertThat(windowed.values).hasSize((int) mask.totalRows());

        assertThat(windowed.decodedValueCount)
                .as("the windowed decode materializes only the selected non-null values")
                .isEqualTo(selectedNonNull)
                .as("the windowed decode materializes far fewer values than the compact path")
                .isLessThan(compact.decodedValueCount);
    }

    @Test
    void windowedDecodeMatchesCompactPathForNullableColumn() throws Exception {
        Path file = writeNullableLongs();
        ParquetSchema schema = flatSchema(optionalInt64("v"));

        // Mask covers null and non-null rows, selected and unselected alike.
        RowRanges mask = new RowRanges(List.of(new Range(3, 6), new Range(20, 23), new Range(48, 49)));
        int selectedNonNull = countSelectedNonNull(mask);

        DrainResult compact = drainCompacting(file, schema, mask);
        DrainResult windowed = drainWindowed(file, schema, mask);

        assertThat(windowed.values).as("emitted values match the compact path").isEqualTo(compact.values);
        assertThat(windowed.nulls).as("emitted nulls match the compact path").isEqualTo(compact.nulls);
        assertThat(windowed.values).hasSize((int) mask.totalRows());

        assertThat(windowed.decodedValueCount)
                .as("the windowed decode materializes only the selected non-null values")
                .isEqualTo(selectedNonNull)
                .as("the windowed decode materializes far fewer values than the compact path")
                .isLessThan(compact.decodedValueCount);
    }

    // --- drains ---

    /** Drains the masked reader that decodes each page whole and compacts it to the mask's surviving rows. */
    private DrainResult drainCompacting(Path file, ParquetSchema schema, RowRanges mask) throws Exception {
        return overColumn(file, schema, mask, ValueDecode.EAGER, MaskedPageSkipParityTest::drainWholePages);
    }

    /** Drains the masked reader that decodes one window of surviving rows at a time out of the page's decoder. */
    private DrainResult drainWindowed(Path file, ParquetSchema schema, RowRanges mask) throws Exception {
        return overColumn(file, schema, mask, ValueDecode.WINDOWED_MASK, MaskedPageSkipParityTest::drainWindows);
    }

    private static DrainResult drainWholePages(BatchColumnReader reader) {
        DrainedRows drained = new DrainedRows();
        List<AutoCloseable> acquiredBuffers = new ArrayList<>();
        while (reader.hasMore()) {
            int pageRows = reader.rowsRemainingInCurrentPage();
            drained.addVector((LongVector) reader.readBatch(pageRows, acquiredBuffers));
        }
        return drained.toResult(reader);
    }

    /** Takes one window per page: every surviving row the page can serve, all of them kept. */
    private static DrainResult drainWindows(BatchColumnReader reader) {
        DrainedRows drained = new DrainedRows();
        List<AutoCloseable> acquiredBuffers = new ArrayList<>();
        while (reader.hasMore()) {
            int windowRows = reader.logicalRowsRemainingInCurrentPage();
            BitSet keepAll = new BitSet(windowRows);
            keepAll.set(0, windowRows);
            BatchColumnReader.MaskedWindow window = reader.readMaskedWindow(windowRows, keepAll, acquiredBuffers);
            drained.addVector((LongVector) window.vector());
        }
        return drained.toResult(reader);
    }

    /** The values and nulls of the emitted rows, in read order. */
    private static final class DrainedRows {

        private final List<Long> values = new ArrayList<>();
        private final List<Boolean> nulls = new ArrayList<>();

        void addVector(LongVector vector) {
            for (int i = 0; i < vector.size(); i++) {
                boolean isNull = vector.isNull(i);
                nulls.add(isNull);
                values.add(isNull ? null : vector.getLong(i));
            }
        }

        DrainResult toResult(BatchColumnReader reader) {
            return new DrainResult(values, nulls, reader.decodedValueCount());
        }
    }

    private record DrainResult(List<Long> values, List<Boolean> nulls, long decodedValueCount) {}

    // --- reader construction ---

    /** What a test does with one open column reader over the file's single row group. */
    @FunctionalInterface
    private interface ColumnDrain {
        DrainResult over(BatchColumnReader reader);
    }

    /**
     * Opens the file's single column under {@code mask} in {@code valueDecode} mode and hands the reader to
     * {@code drain}. The chunk is fetched under the same mask, the narrowed fetch a driver plans when the column index
     * pruned the row group.
     */
    private DrainResult overColumn(
            Path file, ParquetSchema schema, RowRanges mask, ValueDecode valueDecode, ColumnDrain drain)
            throws Exception {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            FileMetaData footer = ParquetFormat.readFooter(source);
            RowGroup rowGroup = footer.rowGroups().get(0);
            OffsetIndex offsetIndex = loadOffsetIndex(source, rowGroup);
            RowGroupChunks chunks = RowGroupChunks.of(rowGroup, schema, indexLoader(source));
            RowGroupFetcher fetcher = TestFetchers.over(source, schema, schema, SegmentPool.getDefault());
            RowGroupSurvivor survivor = new RowGroupSurvivor(chunks, Optional.of(mask), true);
            try (RowGroupFetch fetch =
                    fetcher.fetch(survivor, fetcher.planFor(survivor, Optional.empty()), BudgetReservation.NONE)) {
                FetchedColumnChunk chunk = fetch.columns().get(0);
                BatchColumnReader reader = new BatchColumnReader(
                        TestDecodeBuffers.ample(), chunk, leaf(schema), mask, offsetIndex, valueDecode);
                try {
                    return drain.over(reader);
                } finally {
                    reader.close();
                }
            }
        }
    }

    private static IndexSectionLoader indexLoader(ByteRangeSource source) {
        return new IndexSectionLoader() {
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

    // --- fixtures ---

    private Path writeRequiredLongs() throws Exception {
        Path file = tempDir.resolve("masked-required.parquet");
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
        Path file = tempDir.resolve("masked-nullable.parquet");
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
}
