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
package io.tileverse.parquetry.data;

import static io.tileverse.parquetry.filter.Pred.col;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.EOFException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.internal.filter.bloom.SplitBlockBloomFilter;
import io.tileverse.parquetry.internal.footer.CompactFooter;
import io.tileverse.parquetry.internal.footer.LeafIndex;
import io.tileverse.parquetry.internal.read.IndexSectionLoader;
import io.tileverse.parquetry.internal.read.IndexSectionRange;
import io.tileverse.parquetry.internal.read.RowGroupChunks;
import io.tileverse.parquetry.internal.read.RowMasks;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.observe.FetchAccumulator;
import io.tileverse.parquetry.observe.FetchPurpose;
import io.tileverse.parquetry.observe.FetchStats;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.runtime.ParquetRuntime;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.testsupport.RecordingByteRangeSource;

/**
 * Proves that a phase whose whole set of index sections is known asks the byte source once for all of them, that the
 * per-column memo still serves every later lookup without a read, that the cost of the one call is reported, and that
 * bytes held under an offset serve only a lookup of their own length.
 */
class IndexSectionBatchReadTest {

    private static final ColumnPath A = ColumnPath.of("a");
    private static final ColumnPath B = ColumnPath.of("b");
    private static final ColumnPath C = ColumnPath.of("c");
    private static final ColumnPath ABSENT = ColumnPath.of("no_such_column");

    @TempDir
    Path tempDir;

    @Test
    void warmingThreeOffsetIndexesEntersTheSourceOnce() throws Exception {
        try (Fixture fixture = openThreeColumnFile()) {
            fixture.chunks().warmOffsetIndexes(List.of(A, B, C));

            assertThat(fixture.source().sourceCalls())
                    .as("three offset indexes known together are one call")
                    .isEqualTo(1);
            assertThat(fixture.chunks().offsetIndex(A)).isPresent();
            assertThat(fixture.chunks().offsetIndex(B)).isPresent();
            assertThat(fixture.chunks().offsetIndex(C)).isPresent();
            assertThat(fixture.source().sourceCalls())
                    .as("a warmed section is decoded from bytes already in hand")
                    .isEqualTo(1);
        }
    }

    @Test
    void aMemoizedSectionIsNotWarmedAgain() throws Exception {
        try (Fixture fixture = openThreeColumnFile()) {
            assertThat(fixture.chunks().offsetIndex(A)).isPresent();
            assertThat(fixture.source().sourceCalls()).isEqualTo(1);

            fixture.chunks().warmOffsetIndexes(List.of(A, B, C));

            assertThat(fixture.source().sourceCalls())
                    .as("the warm covers the two paths with no memo entry")
                    .isEqualTo(2);
            assertThat(fixture.chunks().offsetIndex(B)).isPresent();
            assertThat(fixture.chunks().offsetIndex(C)).isPresent();
            assertThat(fixture.source().sourceCalls()).isEqualTo(2);
        }
    }

    @Test
    void warmingPageStatsEntersTheSourceOnceForBothSectionsOfEveryColumn() throws Exception {
        try (Fixture fixture = openThreeColumnFile()) {
            fixture.chunks().warmPageStats(List.of(A, B));

            assertThat(fixture.source().sourceCalls())
                    .as("four sections across two columns are one call")
                    .isEqualTo(1);
            assertThat(fixture.chunks().pageStats(A)).isPresent();
            assertThat(fixture.chunks().pageStats(B)).isPresent();
            assertThat(fixture.source().sourceCalls()).isEqualTo(1);
        }
    }

    @Test
    void aWarmedBatchReportsOneCallAndWhatItCost() throws Exception {
        try (Fixture fixture = openThreeColumnFile()) {
            fixture.chunks().warmPageStats(List.of(A, B));

            FetchStats stats = fixture.accumulator().snapshot();
            assertThat(stats.requestCount())
                    .as("two column indexes and two offset indexes")
                    .isEqualTo(4);
            assertThat(stats.fetchCount())
                    .as("one batch spanning two kinds of section is still one call to the byte source")
                    .isEqualTo(1);
            assertThat(stats.columnIndexBytes()).isPositive();
            assertThat(stats.offsetIndexBytes()).isPositive();
            assertThat(stats.backendFetches())
                    .as("the transport under a file reads at least once and at most once per range")
                    .isBetween(1L, 4L);
            assertThat(stats.bytesTransferred())
                    .as("a file moves exactly the index bytes asked for, bridging no hole")
                    .isEqualTo(stats.columnIndexBytes() + stats.offsetIndexBytes());
            assertThat(stats.bytesFromCache())
                    .as("nothing between the test and the file serves a byte")
                    .isZero();
        }
    }

    @Test
    void aColumnAbsentFromTheRowGroupIsWarmedWithNoRead() throws Exception {
        try (Fixture fixture = openThreeColumnFile()) {
            fixture.chunks().warmPageStats(List.of(ABSENT));

            assertThat(fixture.source().sourceCalls())
                    .as("a path with no chunk contributes no range")
                    .isZero();
        }
    }

    @Test
    void buildingADecodeMaskEntersTheSourceOnceForEveryScanLeaf() throws Exception {
        try (Fixture fixture = openThreeColumnFile()) {
            RowRanges rows = RowRanges.all(8);

            assertThat(RowMasks.maskFor(fixture.chunks(), rows, List.of(A, B, C)))
                    .isPresent();
            assertThat(fixture.source().sourceCalls())
                    .as("the mask knows its leaves before it asks for the first offset index")
                    .isEqualTo(1);
        }
    }

    @Test
    void aFilteredReadWarmsEveryPredicateColumnTheTierConsults() throws Exception {
        Path file = writeThreeColumnFile();
        Predicate predicate = col("a").eq(4L).and(col("b").eq(40L));

        try (RecordingByteRangeSource source = new RecordingByteRangeSource(ByteRangeSource.ofFile(file))) {
            ParquetFileReader reader = ParquetFileReader.open(source, serialRuntime(), Optional.empty());
            source.reset();

            long matched;
            try (Stream<ParquetRecord> rows = reader.read(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
                matched = rows.count();
            }

            assertThat(matched)
                    .as("the filter path ran and kept the one matching row")
                    .isEqualTo(1);
            assertThat(source.sourceCalls())
                    .as("three calls: the tier reads the four index sections of its two predicate columns in one,"
                            + " the mask reads the third leaf's offset index, and the surviving pages are fetched")
                    .isEqualTo(3);
        }
    }

    @Test
    void aHeldSectionOfAnotherLengthIsReadAgainRatherThanDecodedInPlace() throws Exception {
        Path file = writeThreeColumnFile();
        try (RecordingByteRangeSource source = new RecordingByteRangeSource(ByteRangeSource.ofFile(file))) {
            ColumnChunk chunk = ParquetFormat.readFooter(source)
                    .rowGroups()
                    .get(0)
                    .columns()
                    .get(0);
            long offset = chunk.offsetIndexOffset().orElseThrow();
            int length = chunk.offsetIndexLength().orElseThrow();
            OffsetIndex expected = ParquetFormat.readOffsetIndex(source, offset, length);
            FooterIndexSectionLoader loader = new FooterIndexSectionLoader(source, FetchAccumulator.NONE);
            loader.prefetch(List.of(
                    new IndexSectionRange(FetchPurpose.OFFSET_INDEX, offset, length),
                    new IndexSectionRange(FetchPurpose.OFFSET_INDEX, offset, length + 1)));
            source.reset();

            OffsetIndex read = loader.readOffsetIndex(offset, length);

            assertThat(source.sourceCalls())
                    .as("bytes held under this offset span another length; the section is read for itself")
                    .isEqualTo(1);
            assertThat(read).isEqualTo(expected);
        }
    }

    @Test
    void aFailedWarmStillLeavesEverySectionReadable() throws Exception {
        try (Fixture fixture = openThreeColumnFile(FailingPrefetch::new)) {
            fixture.chunks().warmOffsetIndexes(List.of(A, B, C));

            assertThat(fixture.chunks().offsetIndex(A))
                    .as("a warm that fails reads nothing; the section still loads on its own lookup")
                    .isPresent();
        }
    }

    /** A loader whose batch read fails, standing for a footer whose index sections do not lie where it says. */
    private record FailingPrefetch(IndexSectionLoader delegate) implements IndexSectionLoader {

        @Override
        public OffsetIndex readOffsetIndex(long offset, int length) {
            return delegate.readOffsetIndex(offset, length);
        }

        @Override
        public ColumnIndex readColumnIndex(long offset, int length) {
            return delegate.readColumnIndex(offset, length);
        }

        @Override
        public SplitBlockBloomFilter readBloom(long offset, int length) {
            return delegate.readBloom(offset, length);
        }

        @Override
        public void prefetch(List<IndexSectionRange> sections) {
            throw new UncheckedIOException(new EOFException("range past end of source"));
        }
    }

    /** One opened file: the recording source, the accumulating loader, and the first row group's view over them. */
    private record Fixture(RecordingByteRangeSource source, FetchAccumulator accumulator, RowGroupChunks chunks)
            implements AutoCloseable {

        @Override
        public void close() {
            source.close();
        }
    }

    private Fixture openThreeColumnFile() throws Exception {
        return openThreeColumnFile(UnaryOperator.identity());
    }

    private Fixture openThreeColumnFile(UnaryOperator<IndexSectionLoader> decorate) throws Exception {
        Path file = writeThreeColumnFile();
        ParquetSchema schema = threeColumnSchema();
        RecordingByteRangeSource source = new RecordingByteRangeSource(ByteRangeSource.ofFile(file));
        FileMetaData footer = ParquetFormat.readFooter(source);
        LeafIndex leaves = LeafIndex.of(schema);
        FetchAccumulator accumulator = FetchAccumulator.active();
        RowGroupChunks chunks = RowGroupChunks.of(
                CompactFooter.encode(footer, leaves),
                0,
                leaves,
                schema,
                decorate.apply(new FooterIndexSectionLoader(source, accumulator)));
        source.reset();
        return new Fixture(source, accumulator, chunks);
    }

    /** Eight rows of three INT64 columns in one row group, two rows per page, hence four pages per column. */
    private Path writeThreeColumnFile() throws Exception {
        Path file = tempDir.resolve("three-column-int64.parquet");
        ParquetSchema schema = threeColumnSchema();
        WriteOptions options =
                WriteOptions.builder().tempDir(tempDir).pageValueLimit(2).build();
        List<Map<ColumnPath, Object>> rows = new ArrayList<>();
        for (long i = 0; i < 8; i++) {
            Map<ColumnPath, Object> row = new HashMap<>();
            row.put(A, i);
            row.put(B, i * 10);
            row.put(C, i * 100);
            rows.add(row);
        }
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, options)) {
            writer.writeBatch(WriteFixtures.batch(schema, rows));
        }
        return file;
    }

    private static ParquetSchema threeColumnSchema() {
        return flatSchema(requiredInt64("a"), requiredInt64("b"), requiredInt64("c"));
    }

    /** A runtime that neither prefetches nor decodes ahead, which keeps the recorded call count deterministic. */
    private static ParquetRuntime serialRuntime() {
        return ParquetRuntime.builder().maxDecodeAhead(0).prefetchDepth(0).build();
    }

    private static ParquetSchema flatSchema(SchemaNode.Primitive... leaves) {
        List<SchemaNode> children =
                Stream.of(leaves).map(SchemaNode.class::cast).toList();
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, children, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive requiredInt64(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
    }
}
