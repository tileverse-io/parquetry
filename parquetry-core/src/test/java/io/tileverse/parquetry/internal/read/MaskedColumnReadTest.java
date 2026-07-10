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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.IntVector;
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

/** Drives a single masked {@link BatchColumnReader} over a real multi-page chunk. */
class MaskedColumnReadTest {

    private static final ColumnPath V = ColumnPath.of("v");

    @TempDir
    Path tempDir;

    @Test
    void maskedReaderYieldsOnlySurvivingRowsAndSkipsNonSurvivingPages() throws Exception {
        Path file = writeEightRowsAcrossFourPages();
        ParquetSchema schema = flatSchema(requiredInt32("v"));
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            FileMetaData footer = ParquetFormat.readFooter(source);
            RowGroup rowGroup = footer.rowGroups().get(0);
            OffsetIndex offsetIndex = loadOffsetIndex(source, rowGroup);

            // Pages [0,1] [2,3] [4,5] [6,7]; keep rows 4..7 -> last two pages survive, first two are skipped.
            RowRanges surviving = new RowRanges(List.of(new Range(4, 7)));

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
            RowGroupSurvivor survivor = new RowGroupSurvivor(chunks, Optional.of(surviving), true);
            try (RowGroupFetch fetch = fetcher.fetch(survivor, fetcher.planFor(survivor), BudgetReservation.NONE)) {
                FetchedColumnChunk chunk = fetch.columns().get(0);
                BatchColumnReader colReader =
                        new BatchColumnReader(TestDecodeBuffers.ample(), chunk, leaf(schema), surviving, offsetIndex);

                List<Integer> emitted = drainInts(colReader);

                assertThat(emitted).containsExactly(4, 5, 6, 7);
                assertThat(colReader.decodedDataPageCount())
                        .as("only the two surviving pages are decoded")
                        .isEqualTo(2)
                        .isLessThan(offsetIndex.pageLocations().size());
                colReader.close();
            }
        }
    }

    private static List<Integer> drainInts(BatchColumnReader colReader) {
        List<Integer> out = new ArrayList<>();
        List<AutoCloseable> acquiredBuffers = new ArrayList<>();
        while (colReader.hasMore()) {
            int n = colReader.rowsRemainingInCurrentPage();
            ColumnVector vec = colReader.readBatch(n, acquiredBuffers);
            IntVector ints = (IntVector) vec;
            for (int i = 0; i < ints.size(); i++) {
                out.add(ints.getInt(i));
            }
        }
        return out;
    }

    private static OffsetIndex loadOffsetIndex(ByteRangeSource source, RowGroup rowGroup) {
        ColumnChunk chunk = rowGroup.columns().get(0);
        return ParquetFormat.readOffsetIndex(
                source,
                chunk.offsetIndexOffset().getAsLong(),
                chunk.offsetIndexLength().getAsInt());
    }

    private Path writeEightRowsAcrossFourPages() throws Exception {
        Path file = tempDir.resolve("masked-col.parquet");
        ParquetSchema schema = flatSchema(requiredInt32("v"));
        WriteOptions options =
                WriteOptions.builder().tempDir(tempDir).pageValueLimit(2).build();
        List<Map<ColumnPath, Object>> rows = new ArrayList<>();
        for (int v = 0; v < 8; v++) {
            rows.add(Map.of(V, v));
        }
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, options)) {
            writer.writeBatch(WriteFixtures.batch(schema, rows));
        }
        return file;
    }

    private static SchemaNode.Primitive leaf(ParquetSchema schema) {
        return (SchemaNode.Primitive) schema.find(V).orElseThrow();
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
}
