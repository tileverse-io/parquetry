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
package io.tileverse.parquetry.internal.read;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ParquetWriter;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteRow;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.CompressionCodec;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.internal.filter.FilterPipeline.ColumnPageStats;
import io.tileverse.parquetry.internal.filter.bloom.BloomFilterReader;
import io.tileverse.parquetry.internal.filter.bloom.SplitBlockBloomFilter;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Unit tests for {@link RowGroupChunks}. Verifies that index sections are loaded at most once (memoization), that
 * missing index sections degrade cleanly to empty without calling the loader, and that loader failures degrade to empty
 * without propagation.
 */
class RowGroupChunksTest {

    private static final ColumnPath V = ColumnPath.of("v");
    private static final ColumnPath UNKNOWN = ColumnPath.of("no_such_column");

    @TempDir
    Path tempDir;

    /**
     * Verifies that calling {@code pageStats(path)} twice loads the OffsetIndex and ColumnIndex exactly once each, and
     * that a subsequent direct {@code offsetIndex(path)} call does not re-read (the memo is shared).
     */
    @Test
    void pageStatsAndOffsetIndexShareMemo() throws Exception {
        Path file = writeMultiPageInt64File();
        ParquetSchema schema = flatSchema(requiredInt64("v"));

        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            FileMetaData footer = ParquetFormat.readFooter(source);
            RowGroup rowGroup = footer.rowGroups().get(0);
            CountingLoader loader = new CountingLoader(source);
            RowGroupChunks chunks = RowGroupChunks.of(rowGroup, schema, loader);

            Optional<ColumnPageStats> first = chunks.pageStats(V);
            Optional<ColumnPageStats> second = chunks.pageStats(V);

            assertThat(first)
                    .as("pageStats should be present for a column with column+offset index")
                    .isPresent();
            assertThat(first).as("second call should return the same result").isEqualTo(second);
            assertThat(loader.offsetIndexReadCount())
                    .as("OffsetIndex should be loaded exactly once, even after two pageStats calls")
                    .isEqualTo(1);
            assertThat(loader.columnIndexReadCount())
                    .as("ColumnIndex should be loaded exactly once, even after two pageStats calls")
                    .isEqualTo(1);

            // A direct offsetIndex(V) call after pageStats must NOT re-read; the memo from pageStats applies.
            Optional<OffsetIndex> oi = chunks.offsetIndex(V);
            assertThat(oi).as("offsetIndex should be present").isPresent();
            assertThat(loader.offsetIndexReadCount())
                    .as("offsetIndex after pageStats must not issue a second read")
                    .isEqualTo(1);
        }
    }

    /**
     * Verifies that a column chunk without offset or column index offsets returns empty for all index-section methods
     * without calling the loader.
     */
    @Test
    void missingIndexSectionsDegradeToEmptyWithoutCallingLoader() {
        // Construct a minimal RowGroup with a column that has no index offsets.
        ColumnMetaData meta = ColumnMetaData.builder()
                .type(PhysicalType.INT32)
                .encodings(List.of())
                .pathInSchema(List.of("v"))
                .codec(CompressionCodec.UNCOMPRESSED)
                .numValues(1L)
                .totalUncompressedSize(10L)
                .totalCompressedSize(10L)
                .keyValueMetadata(List.of())
                .dataPageOffset(4L)
                .encodingStats(List.of())
                .build();
        ColumnChunk chunk = ColumnChunk.builder()
                .metaData(Optional.of(meta))
                .encryptedColumnMetadata(java.lang.foreign.MemorySegment.NULL)
                .build();
        RowGroup rowGroup = RowGroup.builder()
                .columns(List.of(chunk))
                .totalByteSize(10L)
                .numRows(1L)
                .build();
        ParquetSchema schema = flatSchema(requiredInt32("v"));

        // This loader must never be called; any call is a test failure.
        IndexSectionLoader assertNotCalled = new IndexSectionLoader() {
            @Override
            public OffsetIndex readOffsetIndex(long offset, int length) {
                throw new AssertionError("loader must not be called when index offsets are absent");
            }

            @Override
            public ColumnIndex readColumnIndex(long offset, int length) {
                throw new AssertionError("loader must not be called when index offsets are absent");
            }

            @Override
            public SplitBlockBloomFilter readBloom(long offset, int length) {
                throw new AssertionError("loader must not be called when bloom offset is absent");
            }
        };

        RowGroupChunks chunks = RowGroupChunks.of(rowGroup, schema, assertNotCalled);

        assertThat(chunks.offsetIndex(V))
                .as("offsetIndex should be empty when the chunk has no offsetIndexOffset")
                .isEmpty();
        assertThat(chunks.columnIndex(V))
                .as("columnIndex should be empty when the chunk has no columnIndexOffset")
                .isEmpty();
        assertThat(chunks.pageStats(V))
                .as("pageStats should be empty when either index is absent")
                .isEmpty();
        assertThat(chunks.bloom(V))
                .as("bloom should be empty when the chunk has no bloomFilterOffset")
                .isEmpty();
    }

    /**
     * Verifies that a loader that throws a RuntimeException degrades the result to empty without propagating the
     * exception to the caller.
     */
    @Test
    void loaderFailureDegradesToEmpty() {
        ColumnMetaData meta = ColumnMetaData.builder()
                .type(PhysicalType.INT32)
                .encodings(List.of())
                .pathInSchema(List.of("v"))
                .codec(CompressionCodec.UNCOMPRESSED)
                .numValues(1L)
                .totalUncompressedSize(10L)
                .totalCompressedSize(10L)
                .keyValueMetadata(List.of())
                .dataPageOffset(4L)
                .encodingStats(List.of())
                .bloomFilterOffset(OptionalLong.of(1031L))
                .bloomFilterLength(OptionalLong.of(64L))
                .build();
        ColumnChunk chunk = ColumnChunk.builder()
                .metaData(Optional.of(meta))
                .offsetIndexOffset(OptionalLong.of(999L))
                .offsetIndexLength(OptionalInt.of(16))
                .columnIndexOffset(OptionalLong.of(1015L))
                .columnIndexLength(OptionalInt.of(16))
                .encryptedColumnMetadata(java.lang.foreign.MemorySegment.NULL)
                .build();
        RowGroup rowGroup = RowGroup.builder()
                .columns(List.of(chunk))
                .totalByteSize(10L)
                .numRows(1L)
                .build();
        ParquetSchema schema = flatSchema(requiredInt32("v"));

        IndexSectionLoader alwaysThrows = new IndexSectionLoader() {
            @Override
            public OffsetIndex readOffsetIndex(long offset, int length) {
                throw new RuntimeException("simulated I/O failure");
            }

            @Override
            public ColumnIndex readColumnIndex(long offset, int length) {
                throw new RuntimeException("simulated I/O failure");
            }

            @Override
            public SplitBlockBloomFilter readBloom(long offset, int length) {
                throw new RuntimeException("simulated I/O failure");
            }
        };

        RowGroupChunks chunks = RowGroupChunks.of(rowGroup, schema, alwaysThrows);

        assertThat(chunks.offsetIndex(V))
                .as("offsetIndex should degrade to empty on loader failure")
                .isEmpty();
        assertThat(chunks.columnIndex(V))
                .as("columnIndex should degrade to empty on loader failure")
                .isEmpty();
        assertThat(chunks.pageStats(V))
                .as("pageStats should degrade to empty on loader failure")
                .isEmpty();
        assertThat(chunks.bloom(V))
                .as("bloom should degrade to empty on loader failure")
                .isEmpty();
    }

    /**
     * Verifies that {@code isFlat} and {@code primitiveKind} agree with the schema: present/true for a flat primitive
     * leaf, empty for an unknown path.
     */
    @Test
    void flatnessAndKindMatchSchema() {
        // Build a minimal row group (content doesn't matter for schema queries).
        ColumnMetaData meta = ColumnMetaData.builder()
                .type(PhysicalType.INT64)
                .encodings(List.of())
                .pathInSchema(List.of("v"))
                .codec(CompressionCodec.UNCOMPRESSED)
                .numValues(1L)
                .totalUncompressedSize(10L)
                .totalCompressedSize(10L)
                .keyValueMetadata(List.of())
                .dataPageOffset(4L)
                .encodingStats(List.of())
                .build();
        ColumnChunk chunk = ColumnChunk.builder()
                .metaData(Optional.of(meta))
                .encryptedColumnMetadata(java.lang.foreign.MemorySegment.NULL)
                .build();
        RowGroup rowGroup = RowGroup.builder()
                .columns(List.of(chunk))
                .totalByteSize(10L)
                .numRows(1L)
                .build();
        ParquetSchema schema = flatSchema(requiredInt64("v"));

        IndexSectionLoader noop = new IndexSectionLoader() {
            @Override
            public OffsetIndex readOffsetIndex(long offset, int length) {
                throw new AssertionError("unexpected call");
            }

            @Override
            public ColumnIndex readColumnIndex(long offset, int length) {
                throw new AssertionError("unexpected call");
            }

            @Override
            public SplitBlockBloomFilter readBloom(long offset, int length) {
                throw new AssertionError("unexpected call");
            }
        };

        RowGroupChunks chunks = RowGroupChunks.of(rowGroup, schema, noop);

        assertThat(chunks.isFlat(V))
                .as("a required (non-repeated) column should be considered flat")
                .isTrue();
        assertThat(chunks.primitiveKind(V))
                .as("a known INT64 column should return PrimitiveKind.INT64")
                .contains(PrimitiveKind.INT64);
        assertThat(chunks.primitiveKind(UNKNOWN))
                .as("an unknown column path should return empty from primitiveKind")
                .isEmpty();
    }

    /**
     * Verifies that a bloom-filter length that does not fit an int (a malformed file could record one) degrades to
     * empty instead of letting the conversion overflow escape to the caller.
     */
    @Test
    void bloomLengthOverflowDegradesToEmpty() {
        ColumnMetaData meta = ColumnMetaData.builder()
                .type(PhysicalType.INT32)
                .encodings(List.of())
                .pathInSchema(List.of("v"))
                .codec(CompressionCodec.UNCOMPRESSED)
                .numValues(1L)
                .totalUncompressedSize(10L)
                .totalCompressedSize(10L)
                .keyValueMetadata(List.of())
                .dataPageOffset(4L)
                .encodingStats(List.of())
                .bloomFilterOffset(OptionalLong.of(1031L))
                .bloomFilterLength(OptionalLong.of(Long.MAX_VALUE))
                .build();
        ColumnChunk chunk = ColumnChunk.builder()
                .metaData(Optional.of(meta))
                .encryptedColumnMetadata(java.lang.foreign.MemorySegment.NULL)
                .build();
        RowGroup rowGroup = RowGroup.builder()
                .columns(List.of(chunk))
                .totalByteSize(10L)
                .numRows(1L)
                .build();
        ParquetSchema schema = flatSchema(requiredInt32("v"));

        IndexSectionLoader assertBloomNotReached = new IndexSectionLoader() {
            @Override
            public OffsetIndex readOffsetIndex(long offset, int length) {
                throw new AssertionError("unexpected call");
            }

            @Override
            public ColumnIndex readColumnIndex(long offset, int length) {
                throw new AssertionError("unexpected call");
            }

            @Override
            public SplitBlockBloomFilter readBloom(long offset, int length) {
                throw new AssertionError("the length conversion should fail before the loader is reached");
            }
        };

        RowGroupChunks chunks = RowGroupChunks.of(rowGroup, schema, assertBloomNotReached);

        assertThat(chunks.bloom(V))
                .as("an out-of-range bloom-filter length should degrade to empty, not throw")
                .isEmpty();
    }

    // --- helpers ---

    private Path writeMultiPageInt64File() throws Exception {
        Path file = tempDir.resolve("multi-page-int64.parquet");
        ParquetSchema schema = flatSchema(requiredInt64("v"));
        WriteOptions options =
                WriteOptions.builder().tempDir(tempDir).pageValueLimit(2).build();
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(file), schema, options)) {
            for (long i = 0; i < 8; i++) {
                writer.write(rowOf(Map.of(V, i)));
            }
        }
        return file;
    }

    private static ParquetSchema flatSchema(SchemaNode.Primitive... leaves) {
        List<SchemaNode> children =
                Stream.of(leaves).map(SchemaNode.class::cast).toList();
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, children, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive requiredInt32(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive requiredInt64(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static WriteRow rowOf(Map<ColumnPath, Object> values) {
        Map<ColumnPath, Object> copy = new HashMap<>(values);
        return copy::get;
    }

    /**
     * A delegating {@link IndexSectionLoader} that counts total read calls to each section type. Tests with a single
     * column use the total count to verify memoization prevented duplicate reads.
     */
    private static final class CountingLoader implements IndexSectionLoader {

        private final ByteRangeSource source;
        private int offsetIndexReads = 0;
        private int columnIndexReads = 0;

        CountingLoader(ByteRangeSource source) {
            this.source = source;
        }

        @Override
        public OffsetIndex readOffsetIndex(long offset, int length) {
            offsetIndexReads++;
            return ParquetFormat.readOffsetIndex(source, offset, length);
        }

        @Override
        public ColumnIndex readColumnIndex(long offset, int length) {
            columnIndexReads++;
            return ParquetFormat.readColumnIndex(source, offset, length);
        }

        @Override
        public SplitBlockBloomFilter readBloom(long offset, int length) {
            if (length > 0) {
                return BloomFilterReader.read(source, offset, length);
            }
            return BloomFilterReader.readWithoutLength(source, offset);
        }

        int offsetIndexReadCount() {
            return offsetIndexReads;
        }

        int columnIndexReadCount() {
            return columnIndexReads;
        }
    }
}
