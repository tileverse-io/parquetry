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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DoubleVector;
import io.tileverse.parquetry.columnar.FloatVector;
import io.tileverse.parquetry.columnar.IntSequence;
import io.tileverse.parquetry.columnar.IntVector;
import io.tileverse.parquetry.columnar.Levels;
import io.tileverse.parquetry.columnar.LongVector;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.data.Compression;
import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.filter.RowRanges.Range;
import io.tileverse.parquetry.format.CompressionCodec;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.internal.filter.PageIndex;
import io.tileverse.parquetry.internal.filter.TestPageIndexes;
import io.tileverse.parquetry.internal.read.page.DataPageRun;
import io.tileverse.parquetry.internal.read.page.PageCursor;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaBuilder;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.testsupport.CorpusFixtures;
import io.tileverse.parquetry.testsupport.VectorArrays;

/**
 * Unit tests for {@link BatchColumnReader}. Fixtures are built in-memory: Thrift compact page headers are written with
 * the {@code parquet-format-structures} {@link Util#writePageHeader} serializer (test scope), and value bytes are
 * encoded with the same little-endian helper used in {@link DataPageV1ReaderTest}.
 *
 * <p>Each test uses {@link Arena#ofConfined()} so the test method owns decompressed-page memory and it is released when
 * the method returns.
 */
class BatchColumnReaderTest {

    private static final ColumnPath PATH = ColumnPath.of("val");

    // --- test 1: single page, no nulls, INT32 ---

    @Test
    void readsOnePageIntoIntVector() throws IOException {
        int[] values = {10, 20, 30, 40};
        FetchedColumnChunk chunk = singlePageInt32Chunk(values, /*maxDef*/ 0);
        SchemaNode.Primitive leaf = requiredInt32Leaf();

        BatchColumnReader reader = new BatchColumnReader(TestDecodeBuffers.ample(), chunk, leaf);

        assertThat(reader.hasMore()).isTrue();

        ColumnVector vec = reader.readBatch(10, TestDecodeBuffers.batchBuffers());
        assertThat(vec).isInstanceOf(IntVector.class);
        assertThat(vec.size()).isEqualTo(4);

        Validity validity = vec.validity();
        assertThat(validity.cardinality()).isEqualTo(4);

        IntVector intVec = (IntVector) vec;
        assertThat(VectorArrays.toArray(intVec)).containsExactly(10, 20, 30, 40);

        assertThat(reader.hasMore()).isFalse();
    }

    // --- test 2: two-page chunk, batch stops at page boundary ---

    @Test
    void readBatchStopsAtPageBoundary() throws IOException {
        int[] page1 = {1, 2, 3, 4};
        int[] page2 = {5, 6, 7, 8, 9, 10};
        FetchedColumnChunk chunk = twoPageInt32Chunk(page1, page2);
        SchemaNode.Primitive leaf = requiredInt32Leaf();

        BatchColumnReader reader = new BatchColumnReader(TestDecodeBuffers.ample(), chunk, leaf);

        // Ask for 7 rows - should only get 4 (page 1 has only 4)
        ColumnVector vec1 = reader.readBatch(7, TestDecodeBuffers.batchBuffers());
        assertThat(vec1.size()).isEqualTo(4);
        assertThat(VectorArrays.toArray(((IntVector) vec1))).containsExactly(1, 2, 3, 4);

        // Still has more (page 2 is pending)
        assertThat(reader.hasMore()).isTrue();

        // Ask for 7 again - gets all 6 from page 2
        ColumnVector vec2 = reader.readBatch(7, TestDecodeBuffers.batchBuffers());
        assertThat(vec2.size()).isEqualTo(6);
        assertThat(VectorArrays.toArray(((IntVector) vec2))).containsExactly(5, 6, 7, 8, 9, 10);

        assertThat(reader.hasMore()).isFalse();
    }

    // --- test 3: nullable column produces validity bitmap ---

    @Test
    void nullableColumnProducesValidityBitmap() throws IOException {
        // 5 rows: rows 0, 2, 4 are non-null (def=1), rows 1, 3 are null (def=0).
        // maxDef = 1 means one optional ancestor.
        // RLE-encoded def levels: values 1, 0, 1, 0, 1 for 5 rows.
        // Only 3 values exist in the value section: for the non-null rows.
        int[] nonNullValues = {100, 200, 300};
        byte[] defLevelBytes = rleEncodeBits(new int[] {1, 0, 1, 0, 1}, /*maxLevel*/ 1);
        FetchedColumnChunk chunk = singlePageNullableChunk(nonNullValues, defLevelBytes, /*numValues*/ 5);
        SchemaNode.Primitive leaf = optionalInt32Leaf();

        BatchColumnReader reader = new BatchColumnReader(TestDecodeBuffers.ample(), chunk, leaf);
        ColumnVector vec = reader.readBatch(10, TestDecodeBuffers.batchBuffers());

        assertThat(vec.size()).isEqualTo(5);
        Validity validity = vec.validity();
        // Rows 0, 2, 4 are non-null.
        assertThat(validity.isValid(0)).isTrue();
        assertThat(validity.isValid(1)).isFalse();
        assertThat(validity.isValid(2)).isTrue();
        assertThat(validity.isValid(3)).isFalse();
        assertThat(validity.isValid(4)).isTrue();
    }

    /**
     * Flat optional INT32 column ({@code maxRep == 0}, {@code maxDef == 1}) with nulls spread across two pages, read in
     * one batch per page. This exercises the origin-validity fast path that decodes the def-level stream straight into
     * an off-heap validity bitmap: the per-slice {@link Validity} and the values must both be correct on each page.
     */
    @Test
    void flatOptionalColumnWithNullsAcrossPageBoundary() throws IOException {
        // Page 1: rows {v, null, v, null} -> def levels {1,0,1,0}, non-null values {10, 20}.
        // Page 2: rows {null, v, v} -> def levels {0,1,1}, non-null values {30, 40}.
        byte[] page1 = nullableInt32Page(new int[] {1, 0, 1, 0}, new int[] {10, 20});
        byte[] page2 = nullableInt32Page(new int[] {0, 1, 1}, new int[] {30, 40});
        FetchedColumnChunk chunk = twoPageChunk(page1, page2, /*totalValues*/ 7, /*maxDef*/ 1);
        SchemaNode.Primitive leaf = optionalInt32Leaf();

        BatchColumnReader reader = new BatchColumnReader(TestDecodeBuffers.ample(), chunk, leaf);

        IntVector first = (IntVector) reader.readBatch(10, TestDecodeBuffers.batchBuffers());
        assertThat(first.size()).isEqualTo(4);
        assertThat(first.validity().isValid(0)).isTrue();
        assertThat(first.getInt(0)).isEqualTo(10);
        assertThat(first.validity().isValid(1)).isFalse();
        assertThat(first.validity().isValid(2)).isTrue();
        assertThat(first.getInt(2)).isEqualTo(20);
        assertThat(first.validity().isValid(3)).isFalse();

        IntVector second = (IntVector) reader.readBatch(10, TestDecodeBuffers.batchBuffers());
        assertThat(second.size()).isEqualTo(3);
        assertThat(second.validity().isValid(0)).isFalse();
        assertThat(second.validity().isValid(1)).isTrue();
        assertThat(second.getInt(1)).isEqualTo(30);
        assertThat(second.validity().isValid(2)).isTrue();
        assertThat(second.getInt(2)).isEqualTo(40);

        assertThat(reader.hasMore()).isFalse();
    }

    /**
     * Regression: a REQUIRED primitive nested under an OPTIONAL struct has the same level shape as a top-level flat
     * optional column ({@code maxRep == 0}, {@code maxDef == 1}; the optional struct contributes the one definition
     * level), but its def-level stream is consumed downstream - {@code DremelAssembler} rebuilds the struct's per-row
     * null mask from it. The origin-validity fast path must NOT engage for a non-top-level leaf; the def-level stream
     * must still be retained. Asserting {@link BatchColumnReader#currentPageDefLevels()} is non-null locks the scoping.
     */
    @Test
    void nestedRequiredLeafUnderOptionalStructRetainsDefLevels() throws IOException {
        // info is null on rows 0 and 2 (descendant def level 0); present on rows 1, 3 (def level 1).
        byte[] defLevelBytes = rleEncodeBits(new int[] {0, 1, 0, 1}, /*maxLevel*/ 1);
        int[] nonNullValues = {11, 33};
        ColumnPath nestedPath = ColumnPath.of("info", "value");
        FetchedColumnChunk chunk = nestedNullableChunk(nestedPath, nonNullValues, defLevelBytes, /*numValues*/ 4);
        SchemaNode.Primitive leaf = new SchemaNode.Primitive(
                "value", Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);

        BatchColumnReader reader = new BatchColumnReader(TestDecodeBuffers.ample(), chunk, leaf);

        Levels defLevels = reader.currentPageDefLevels();
        assertThat(defLevels)
                .as("a non-top-level leaf must retain its def-level stream for struct null reconstruction")
                .isNotNull();
        assertThat(intsOf(defLevels)).containsExactly(0, 1, 0, 1);

        IntVector vec = (IntVector) reader.readBatch(10, TestDecodeBuffers.batchBuffers());
        assertThat(vec.size()).isEqualTo(4);
        assertThat(vec.validity().isValid(0))
                .as("row 0 struct absent -> leaf null")
                .isFalse();
        assertThat(vec.validity().isValid(1)).isTrue();
        assertThat(vec.getInt(1)).isEqualTo(11);
        assertThat(vec.validity().isValid(2)).isFalse();
        assertThat(vec.validity().isValid(3)).isTrue();
        assertThat(vec.getInt(3)).isEqualTo(33);
    }

    /**
     * A nested optional leaf read across two pages: the reader's def-level scratch is reused for page 2, and the page-1
     * values and validity must already be consumed correctly before the reuse overwrites the buffer.
     */
    @Test
    void nestedLeafDefLevelsSurviveScratchReuseAcrossPages() throws IOException {
        byte[] page1 = nullableInt32Page(new int[] {0, 1, 1}, new int[] {11, 22});
        byte[] page2 = nullableInt32Page(new int[] {1, 0, 1, 1}, new int[] {33, 44, 55});
        ColumnPath nestedPath = ColumnPath.of("info", "value");
        FetchedColumnChunk chunk =
                heapChunk(nestedPath, concat(page1, page2), /*numValues*/ 7, /*maxRep*/ 0, /*maxDef*/ 1);
        SchemaNode.Primitive leaf = new SchemaNode.Primitive(
                "value", Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);

        SegmentPool pool = SegmentPool.create();
        BatchColumnReader reader = new BatchColumnReader(TestDecodeBuffers.ample(pool), chunk, leaf);

        assertThat(intsOf(reader.currentPageDefLevels())).containsExactly(0, 1, 1);
        IntVector first = (IntVector) reader.readBatch(10, TestDecodeBuffers.batchBuffers());
        assertThat(first.size()).isEqualTo(3);
        assertThat(first.validity().isValid(0)).isFalse();
        assertThat(first.getInt(1)).isEqualTo(11);
        assertThat(first.getInt(2)).isEqualTo(22);

        // The next currentPageDefLevels call is what loads page 2. The def-level scratch and the page-values scratch
        // already fit page 2 and draw nothing from the pool; the load borrows only the page's decompressed buffer. A
        // delta of exactly one pins both reuses.
        long borrowsBeforePageTwoLoad = pool.stats().totalBorrows();
        assertThat(intsOf(reader.currentPageDefLevels()))
                .as("page 2 decoded into the reused scratch")
                .containsExactly(1, 0, 1, 1);
        assertThat(pool.stats().totalBorrows())
                .as("loading page 2 reuses the def-level and page-values scratches; only the page buffer is borrowed")
                .isEqualTo(borrowsBeforePageTwoLoad + 1);
        IntVector second = (IntVector) reader.readBatch(10, TestDecodeBuffers.batchBuffers());
        assertThat(second.size()).isEqualTo(4);
        assertThat(second.getInt(0)).isEqualTo(33);
        assertThat(second.validity().isValid(1)).isFalse();
        assertThat(second.getInt(2)).isEqualTo(44);
        assertThat(second.getInt(3)).isEqualTo(55);
        assertThat(reader.hasMore()).isFalse();
    }

    private static int[] intsOf(Levels levels) {
        int[] out = new int[levels.size()];
        for (int i = 0; i < out.length; i++) {
            out[i] = levels.get(i);
        }
        return out;
    }

    /**
     * Closing a reader returns its scratch buffers to the pool, also after a page whose value decode threw. The page
     * declares two non-null INT32 values but provides bytes for only one; the def-level decode succeeds, the PLAIN
     * value decode throws.
     */
    @Test
    void failedDecodeReleasesOnClose() throws IOException {
        byte[] defBytes = rleEncodeBits(new int[] {1, 0, 1, 0}, /*maxLevel*/ 1);
        byte[] truncatedValueBytes = encodeInt32sLittleEndian(new int[] {10});
        byte[] payload = buildV1PayloadWithDefLevels(defBytes, truncatedValueBytes);
        byte[] chunkBuffer = encodeV1Page(4, payload, org.apache.parquet.format.Encoding.PLAIN);
        FetchedColumnChunk chunk = heapChunk(PATH, chunkBuffer, 4, /*maxRep*/ 0, /*maxDef*/ 1);

        SegmentPool pool = SegmentPool.create();
        BatchColumnReader reader = new BatchColumnReader(TestDecodeBuffers.ample(pool), chunk, optionalInt32Leaf());

        BatchBuffers acquiredBuffers = TestDecodeBuffers.batchBuffers();

        assertThatThrownBy(() -> reader.readBatch(10, acquiredBuffers)).isInstanceOf(RuntimeException.class);
        reader.close();
        assertThat(pool.stats().outstandingBorrows())
                .as("the reader's scratch buffers must be back after close")
                .isZero();
    }

    /**
     * Reads a null-bearing flat optional page in 3-row batches: slice starts 3 and 9 are not byte-aligned in the page's
     * off-heap origin bitmap, driving the shift-combined arm of the per-slice presence copy. Every row's validity and
     * value must survive the rebasing.
     */
    @Test
    void flatOptionalColumnReadInUnalignedBatches() throws IOException {
        int[] defLevels = {1, 0, 1, 1, 0, 1, 1, 1, 0, 1, 0, 1, 1};
        int[] nonNullValues = {10, 20, 30, 40, 50, 60, 70, 80, 90};
        byte[] page = nullableInt32Page(defLevels, nonNullValues);
        FetchedColumnChunk chunk = heapChunk(PATH, page, defLevels.length, /*maxRep*/ 0, /*maxDef*/ 1);

        BatchColumnReader reader = new BatchColumnReader(TestDecodeBuffers.ample(), chunk, optionalInt32Leaf());

        int row = 0;
        int nextValue = 0;
        while (reader.hasMore()) {
            IntVector vec = (IntVector) reader.readBatch(3, TestDecodeBuffers.batchBuffers());
            for (int i = 0; i < vec.size(); i++, row++) {
                boolean expectedPresent = defLevels[row] == 1;
                assertThat(vec.validity().isValid(i)).as("row %d presence", row).isEqualTo(expectedPresent);
                if (expectedPresent) {
                    assertThat(vec.getInt(i)).as("row %d value", row).isEqualTo(nonNullValues[nextValue++]);
                }
            }
        }
        assertThat(row).isEqualTo(defLevels.length);
        assertThat(nextValue).isEqualTo(nonNullValues.length);
    }

    /** A flat optional page whose every row is null: zero value bytes, validity all-null, no value ever read. */
    @Test
    void fullyNullFlatOptionalPage() throws IOException {
        int[] defLevels = {0, 0, 0, 0, 0};
        byte[] page = nullableInt32Page(defLevels, new int[0]);
        FetchedColumnChunk chunk = heapChunk(PATH, page, defLevels.length, /*maxRep*/ 0, /*maxDef*/ 1);

        BatchColumnReader reader = new BatchColumnReader(TestDecodeBuffers.ample(), chunk, optionalInt32Leaf());

        IntVector vec = (IntVector) reader.readBatch(10, TestDecodeBuffers.batchBuffers());
        assertThat(vec.size()).isEqualTo(5);
        for (int i = 0; i < 5; i++) {
            assertThat(vec.validity().isValid(i)).as("row %d", i).isFalse();
        }
        assertThat(reader.hasMore()).isFalse();
    }

    /**
     * An all-valid flat optional page collapses its origin validity to the bitmap-free representation. Mid-page (one
     * 2-row batch consumed of 5) the pool lends three buffers: the batch's slab, the live page's decompressed buffer
     * and the reader's validity scratch. Closing the batch and the reader returns them all.
     */
    @Test
    void allValidOptionalPageBorrows() throws IOException {
        int[] defLevels = {1, 1, 1, 1, 1};
        int[] values = {1, 2, 3, 4, 5};
        byte[] page = nullableInt32Page(defLevels, values);
        FetchedColumnChunk chunk = heapChunk(PATH, page, defLevels.length, /*maxRep*/ 0, /*maxDef*/ 1);

        SegmentPool pool = SegmentPool.create();
        DecodeBufferAllocator allocator = TestDecodeBuffers.ample(pool);
        BatchColumnReader reader = new BatchColumnReader(allocator, chunk, optionalInt32Leaf());

        BatchBuffers acquired = new BatchBuffers(allocator);
        IntVector vec = (IntVector) reader.readBatch(2, acquired);
        assertThat(vec.getInt(0)).isEqualTo(1);
        assertThat(vec.getInt(1)).isEqualTo(2);
        assertThat(vec.validity().hasNulls()).isFalse();
        assertThat(pool.stats().outstandingBorrows())
                .as("mid-page: the batch's slab, the page buffer and the validity scratch")
                .isEqualTo(3);

        acquired.close();
        reader.close();
        assertThat(pool.stats().outstandingBorrows()).isZero();
    }

    // --- test 4: hasMore false after all pages consumed ---

    @Test
    void hasMoreFalseAfterAllPagesConsumed() throws IOException {
        int[] page1 = {1, 2};
        int[] page2 = {3, 4};
        FetchedColumnChunk chunk = twoPageInt32Chunk(page1, page2);
        SchemaNode.Primitive leaf = requiredInt32Leaf();

        BatchColumnReader reader = new BatchColumnReader(TestDecodeBuffers.ample(), chunk, leaf);

        assertThat(reader.hasMore()).isTrue();
        reader.readBatch(10, TestDecodeBuffers.batchBuffers()); // drains page 1
        assertThat(reader.hasMore()).isTrue();
        reader.readBatch(10, TestDecodeBuffers.batchBuffers()); // drains page 2
        assertThat(reader.hasMore()).isFalse();
    }

    // --- test 5: dictionary-encoded column resolves indices through the dictionary ---

    @Test
    void readsDictionaryEncodedColumn() throws IOException {
        // Three unique values: 100, 200, 300. Index page references them as 0,1,2,1,0.
        // Expected materialized output: 100, 200, 300, 200, 100.
        FetchedColumnChunk chunk = dictionaryEncodedInt32Chunk(new int[] {100, 200, 300}, new int[] {0, 1, 2, 1, 0});

        BatchColumnReader reader = new BatchColumnReader(TestDecodeBuffers.ample(), chunk, requiredInt32Leaf());

        ColumnVector vec = reader.readBatch(10, TestDecodeBuffers.batchBuffers());
        assertThat(vec).isInstanceOf(IntVector.class);
        assertThat(vec.size()).isEqualTo(5);
        assertThat(VectorArrays.toArray(((IntVector) vec))).containsExactly(100, 200, 300, 200, 100);

        assertThat(reader.hasMore()).isFalse();
    }

    // --- test 5b: PLAIN all-valid DOUBLE column read row-by-row across a page boundary ---

    /**
     * Drives the PLAIN all-valid DOUBLE path that slices straight from the live page segment instead of a heap
     * {@code double[]}. Two pages with no nulls are read one row at a time via {@link DoubleVector#getDouble(int)}; the
     * boundary forces a page advance mid-read, exercising the live-page lifetime, and every value must round-trip.
     */
    @Test
    void plainAllValidDoubleColumnReadsAcrossPageBoundary() throws IOException {
        double[] page1 = {0.0, 1.0, 2.0, 3.0, 4.0};
        double[] page2 = {5.0, 6.0, 7.0, 8.0, 9.0, 10.0};
        FetchedColumnChunk chunk = twoPageDoubleChunk(page1, page2);

        BatchColumnReader reader = new BatchColumnReader(TestDecodeBuffers.ample(), chunk, requiredDoubleLeaf());

        List<Double> readValues = new ArrayList<>();
        while (reader.hasMore()) {
            DoubleVector vec = (DoubleVector) reader.readBatch(1, TestDecodeBuffers.batchBuffers());
            assertThat(vec.size()).isEqualTo(1);
            readValues.add(vec.getDouble(0));
        }

        assertThat(readValues).containsExactly(0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0);
    }

    // --- test 5c: PLAIN all-valid INT32 column read row-by-row across a page boundary ---

    /**
     * Drives the PLAIN all-valid INT32 path that slices straight from the live page segment instead of a heap
     * {@code int[]}. Two pages with no nulls are read one row at a time via {@link IntVector#getInt(int)}; the boundary
     * forces a page advance mid-read, exercising the live-page lifetime, and every value must round-trip.
     */
    @Test
    void plainAllValidIntColumnReadsAcrossPageBoundary() throws IOException {
        int[] page1 = {0, 1, 2, 3, 4};
        int[] page2 = {5, 6, 7, 8, 9, 10};
        FetchedColumnChunk chunk = twoPageInt32Chunk(page1, page2);

        BatchColumnReader reader = new BatchColumnReader(TestDecodeBuffers.ample(), chunk, requiredInt32Leaf());

        List<Integer> readValues = new ArrayList<>();
        while (reader.hasMore()) {
            IntVector vec = (IntVector) reader.readBatch(1, TestDecodeBuffers.batchBuffers());
            assertThat(vec.size()).isEqualTo(1);
            readValues.add(vec.getInt(0));
        }

        assertThat(readValues).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    // --- test 5d: PLAIN all-valid INT64 column read row-by-row across a page boundary ---

    /**
     * Drives the PLAIN all-valid INT64 path that slices straight from the live page segment instead of a heap
     * {@code long[]}. Two pages with no nulls are read one row at a time via {@link LongVector#getLong(int)}; the
     * boundary forces a page advance mid-read, exercising the live-page lifetime, and every value must round-trip.
     */
    @Test
    void plainAllValidLongColumnReadsAcrossPageBoundary() throws IOException {
        long[] page1 = {0L, 1L, 2L, 3L, 4L};
        long[] page2 = {5L, 6L, 7L, 8L, 9L, 10L};
        FetchedColumnChunk chunk = twoPageInt64Chunk(page1, page2);

        BatchColumnReader reader = new BatchColumnReader(TestDecodeBuffers.ample(), chunk, requiredLongLeaf());

        List<Long> readValues = new ArrayList<>();
        while (reader.hasMore()) {
            LongVector vec = (LongVector) reader.readBatch(1, TestDecodeBuffers.batchBuffers());
            assertThat(vec.size()).isEqualTo(1);
            readValues.add(vec.getLong(0));
        }

        assertThat(readValues).containsExactly(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);
    }

    // --- test 5e: PLAIN all-valid FLOAT column read row-by-row across a page boundary ---

    /**
     * Drives the PLAIN all-valid FLOAT path that slices straight from the live page segment instead of a heap
     * {@code float[]}. Two pages with no nulls are read one row at a time via {@link FloatVector#getFloat(int)}; the
     * boundary forces a page advance mid-read, exercising the live-page lifetime, and every value must round-trip.
     */
    @Test
    void plainAllValidFloatColumnReadsAcrossPageBoundary() throws IOException {
        float[] page1 = {0.0f, 1.0f, 2.0f, 3.0f, 4.0f};
        float[] page2 = {5.0f, 6.0f, 7.0f, 8.0f, 9.0f, 10.0f};
        FetchedColumnChunk chunk = twoPageFloatChunk(page1, page2);

        BatchColumnReader reader = new BatchColumnReader(TestDecodeBuffers.ample(), chunk, requiredFloatLeaf());

        List<Float> readValues = new ArrayList<>();
        while (reader.hasMore()) {
            FloatVector vec = (FloatVector) reader.readBatch(1, TestDecodeBuffers.batchBuffers());
            assertThat(vec.size()).isEqualTo(1);
            readValues.add(vec.getFloat(0));
        }

        assertThat(readValues).containsExactly(0.0f, 1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f, 8.0f, 9.0f, 10.0f);
    }

    // --- test 5f: unmasked nullable fixed-width pages decode into the pooled page-values segment ---

    /**
     * Pins the zero placeholder for null slots in the pooled page-values segment. A fresh heap array was zero-filled by
     * the JVM; a pooled segment may hold stale bytes from a previous borrower. The pool is smallest-fit over one free
     * list, hence TWO 16KB blocks are pre-dirtied with 0xFF (borrowed simultaneously - a sequential borrow/close pair
     * would reuse one block): the page's small validity-bitmap acquire runs first and consumes one dirty block, leaving
     * the other, still dirty, for the page-values acquire. The validity-blind {@link IntVector#asArray()} read of a
     * null row's slot must yield 0, never the dirty bytes.
     */
    @Test
    void nullablePageNullSlotsReadZeroNotStalePoolBytes() throws IOException {
        int rows = 4096; // 16KB of INT32
        SegmentPool pool = SegmentPool.create();
        seedTwoDirtyBlocks(pool, (long) rows * Integer.BYTES);
        assertThat(pool.stats().freeSegments())
                .as("both dirty blocks must be retained or this test pins nothing")
                .isEqualTo(2);

        int[] defLevels = new int[rows];
        int[] nonNullValues = new int[rows - 2];
        int v = 0;
        for (int row = 0; row < rows; row++) {
            boolean isNull = (row == 100 || row == 4000);
            defLevels[row] = isNull ? 0 : 1;
            if (!isNull) {
                nonNullValues[v++] = row + 1;
            }
        }
        byte[] page = nullableInt32Page(defLevels, nonNullValues);
        FetchedColumnChunk chunk = heapChunk(PATH, page, rows, /*maxRep*/ 0, /*maxDef*/ 1);
        BatchColumnReader reader = new BatchColumnReader(TestDecodeBuffers.ample(pool), chunk, optionalInt32Leaf());

        IntVector vec = (IntVector) reader.readBatch(rows, TestDecodeBuffers.batchBuffers());

        int[] slots = VectorArrays.toArray(vec);
        assertThat(slots[100]).as("null slot reads the zero placeholder").isZero();
        assertThat(slots[4000]).as("null slot reads the zero placeholder").isZero();
        assertThat(slots[0]).isEqualTo(1);
        assertThat(slots[101]).isEqualTo(102); // the value at row 101 is row+1; the null at 100 must not shift it
        assertThat(slots[4095]).isEqualTo(4096);
    }

    /**
     * A nullable DOUBLE page decodes into the pooled segment (not PLAIN-all-valid, hence not the live-page path); the
     * in-place spread must land every value on its own row slot with null slots zeroed.
     */
    @Test
    void nullableDoublePageSpreadsValuesToRowSlots() throws IOException {
        // five rows with rows 1 and 2 null: the def-level stream marks them absent and only three values exist
        byte[] page = nullableDoublePage(new int[] {1, 0, 0, 1, 1}, new double[] {1.5, 2.5, 3.5});
        FetchedColumnChunk chunk = heapChunk(PATH, page, /*numValues*/ 5, /*maxRep*/ 0, /*maxDef*/ 1);
        BatchColumnReader reader = new BatchColumnReader(TestDecodeBuffers.ample(), chunk, optionalDoubleLeaf());

        DoubleVector vec = (DoubleVector) reader.readBatch(10, TestDecodeBuffers.batchBuffers());

        double[] slots = VectorArrays.toArray(vec);
        assertThat(slots[0]).isEqualTo(1.5);
        assertThat(slots[1]).isZero();
        assertThat(slots[2]).isZero();
        assertThat(slots[3]).isEqualTo(2.5);
        assertThat(slots[4]).isEqualTo(3.5);
    }

    /**
     * Pins the all-null lane's explicit zero fill of the pooled page-values segment. No value decode and no spread run
     * for a page with zero non-null values; only the fill stands between a previous borrower's bytes and the
     * validity-blind reads. Same two-dirty-block seeding as the null-slots test: the validity-bitmap acquire consumes
     * one block, the page-values acquire receives the other, still dirty.
     */
    @Test
    void allNullPageValuesAreZeroFilledNotStalePoolBytes() throws IOException {
        int rows = 4096; // 16KB of INT32
        SegmentPool pool = SegmentPool.create();
        seedTwoDirtyBlocks(pool, (long) rows * Integer.BYTES);
        assertThat(pool.stats().freeSegments())
                .as("both dirty blocks must be retained or this test pins nothing")
                .isEqualTo(2);

        int[] defLevels = new int[rows]; // all zero: every row is null
        byte[] page = nullableInt32Page(defLevels, new int[0]);
        FetchedColumnChunk chunk = heapChunk(PATH, page, rows, /*maxRep*/ 0, /*maxDef*/ 1);
        BatchColumnReader reader = new BatchColumnReader(TestDecodeBuffers.ample(pool), chunk, optionalInt32Leaf());

        IntVector vec = (IntVector) reader.readBatch(rows, TestDecodeBuffers.batchBuffers());

        assertThat(VectorArrays.toArray(vec))
                .as("every null slot reads the zero placeholder")
                .containsOnly(0);
    }

    /**
     * Leaves two free blocks of {@code blockBytes} filled with 0xFF in the pool. The blocks are borrowed simultaneously
     * before dirtying; a sequential borrow/close pair would dirty the same block twice.
     */
    private static void seedTwoDirtyBlocks(SegmentPool pool, long blockBytes) {
        SegmentPool.Pooled first = pool.borrow(blockBytes);
        SegmentPool.Pooled second = pool.borrow(blockBytes);
        first.segment().fill((byte) 0xFF);
        second.segment().fill((byte) 0xFF);
        first.close();
        second.close();
    }

    // --- test 6: PLAIN BYTE_ARRAY with nulls, split across batches and a page boundary ---

    /**
     * Locks the behavior the shared-buffer freeze must preserve: a non-dictionary BYTE_ARRAY column with interleaved
     * nulls, read in several {@link BatchColumnReader#readBatch} calls that split a single page and cross a page
     * boundary, must return exactly the original bytes and null pattern per slice.
     */
    @Nested
    class PlainByteArrayWithNulls {

        @Test
        void splitsWithinAPageAndAcrossThePageBoundary() throws IOException {
            // Page 1: 5 rows, present pattern 1,0,1,0,1 -> values "aa", null, "bbbb", null, "c".
            // Page 2: 3 rows, present pattern 0,1,1 -> null, "dd", "eee".
            byte[][] page1 = {bytes("aa"), null, bytes("bbbb"), null, bytes("c")};
            byte[][] page2 = {null, bytes("dd"), bytes("eee")};
            FetchedColumnChunk chunk = twoPageNullableByteArrayChunk(page1, page2);

            BatchColumnReader reader = new BatchColumnReader(TestDecodeBuffers.ample(), chunk, optionalByteArrayLeaf());

            // First batch: 3 of page 1's rows.
            ColumnVector batch1 = reader.readBatch(3, TestDecodeBuffers.batchBuffers());
            assertByteArraySlice(batch1, new byte[][] {bytes("aa"), null, bytes("bbbb")});

            // Second batch asks for 5 but only 2 remain in page 1.
            ColumnVector batch2 = reader.readBatch(5, TestDecodeBuffers.batchBuffers());
            assertByteArraySlice(batch2, new byte[][] {null, bytes("c")});

            assertThat(reader.hasMore()).isTrue();

            // Third batch drains page 2.
            ColumnVector batch3 = reader.readBatch(10, TestDecodeBuffers.batchBuffers());
            assertByteArraySlice(batch3, new byte[][] {null, bytes("dd"), bytes("eee")});

            assertThat(reader.hasMore()).isFalse();
        }

        /**
         * Exercises the direct-decode branches the interleaved-null test leaves untouched: an all-present page whose
         * dense offsets become the row offsets unchanged, and an all-null page whose backing is empty. Both cross a
         * page boundary across more than one {@link BatchColumnReader#readBatch} call.
         */
        @Test
        void handlesAllPresentAndAllNullPages() throws IOException {
            byte[][] page1 = {bytes("aa"), bytes("bbbb"), bytes("c")};
            byte[][] page2 = {null, null};
            FetchedColumnChunk chunk = twoPageNullableByteArrayChunk(page1, page2);

            BatchColumnReader reader = new BatchColumnReader(TestDecodeBuffers.ample(), chunk, optionalByteArrayLeaf());

            // Page 1 is all-present: first batch takes 2 of its 3 rows.
            ColumnVector batch1 = reader.readBatch(2, TestDecodeBuffers.batchBuffers());
            assertByteArraySlice(batch1, new byte[][] {bytes("aa"), bytes("bbbb")});

            // Second batch asks for 2 but only page 1's last row remains; readBatch never crosses a page.
            ColumnVector batch2 = reader.readBatch(2, TestDecodeBuffers.batchBuffers());
            assertByteArraySlice(batch2, new byte[][] {bytes("c")});

            assertThat(reader.hasMore()).isTrue();

            // Page 2 is all-null: an empty backing with two null rows.
            ColumnVector batch3 = reader.readBatch(2, TestDecodeBuffers.batchBuffers());
            assertByteArraySlice(batch3, new byte[][] {null, null});

            assertThat(reader.hasMore()).isFalse();
        }

        /**
         * Pins the per-page refill of the direct-binary row layout: two PLAIN pages that BOTH interleave nulls, read
         * one batch per page, must each return exactly their own bytes and null pattern. The second page's layout must
         * not leak anything from the first page's decode.
         */
        @Test
        void directBinaryScratchesRefillAcrossPages() throws IOException {
            byte[][] page1 = {bytes("alpha"), null, bytes("bravo")};
            byte[][] page2 = {null, bytes("charlie"), bytes("x"), null};
            FetchedColumnChunk chunk = twoPageNullableByteArrayChunk(page1, page2);

            BatchColumnReader reader = new BatchColumnReader(TestDecodeBuffers.ample(), chunk, optionalByteArrayLeaf());

            // readBatch never crosses a page, leaving one batch per page.
            ColumnVector batch1 = reader.readBatch(10, TestDecodeBuffers.batchBuffers());
            assertByteArraySlice(batch1, page1);

            ColumnVector batch2 = reader.readBatch(10, TestDecodeBuffers.batchBuffers());
            assertByteArraySlice(batch2, page2);

            assertThat(reader.hasMore()).isFalse();
        }

        @Test
        void decodesValueBytesOffHeap() throws IOException {
            byte[][] rows = {bytes("aa"), bytes("bbbb"), bytes("c")};
            FetchedColumnChunk chunk = twoPageNullableByteArrayChunk(rows, new byte[][] {bytes("z")});

            BatchColumnReader reader = new BatchColumnReader(TestDecodeBuffers.ample(), chunk, optionalByteArrayLeaf());
            BinaryVector vec = (BinaryVector) reader.readBatch(3, TestDecodeBuffers.batchBuffers());

            assertThat(vec.size()).isEqualTo(3);
            assertThat(vec.approximateHeapBytes())
                    .as("value bytes and offsets live off-heap; only validity is heap")
                    .isEqualTo(vec.validity().heapBytes());
        }

        /**
         * The direct lane parks its row positions and offsets in one pooled page-lifetime segment. A batch that leaves
         * the page partially consumed keeps that borrow parked; closing the reader (and the batch's own valve buffers)
         * must return every borrow to the pool - a non-zero count would mean the page-metadata segment leaked.
         */
        @Test
        void directBinaryPageMetadataBorrowIsReleasedOnClose() throws Exception {
            byte[][] page1 = {bytes("aa"), null, bytes("bbbb"), bytes("c")};
            byte[][] page2 = {null, bytes("dd")};
            FetchedColumnChunk chunk = twoPageNullableByteArrayChunk(page1, page2);

            SegmentPool pool = SegmentPool.create();
            DecodeBufferAllocator allocator = TestDecodeBuffers.ample(pool);
            BatchColumnReader reader = new BatchColumnReader(allocator, chunk, optionalByteArrayLeaf());

            BatchBuffers acquired = new BatchBuffers(allocator);
            ColumnVector batch = reader.readBatch(2, acquired);
            assertByteArraySlice(batch, new byte[][] {bytes("aa"), null});

            reader.close();
            acquired.close();
            assertThat(pool.stats().outstandingBorrows())
                    .as("every pooled borrow, the page-metadata segment included, must be back after close")
                    .isZero();
        }

        private void assertByteArraySlice(ColumnVector vec, byte[][] expected) {
            assertThat(vec).isInstanceOf(BinaryVector.class);
            assertThat(vec.size()).isEqualTo(expected.length);
            BinaryVector binary = (BinaryVector) vec;
            for (int row = 0; row < expected.length; row++) {
                if (expected[row] == null) {
                    assertThat(binary.validity().isValid(row))
                            .as("row %d is null", row)
                            .isFalse();
                } else {
                    assertThat(binary.validity().isValid(row))
                            .as("row %d is present", row)
                            .isTrue();
                    assertThat(binary.get(row).toArray(JAVA_BYTE))
                            .as("row %d bytes", row)
                            .containsExactly(expected[row]);
                }
            }
        }
    }

    // --- test 7: dictionary-encoded BYTE_ARRAY with nulls, repeated values, split across batches ---

    /**
     * Locks the shared-entries dictionary layout for BYTE_ARRAY: a dictionary-encoded column with interleaved nulls and
     * repeated values, read across more than one {@link BatchColumnReader#readBatch} call, must return exactly the
     * original bytes and null pattern, and two rows that reference the same dictionary entry must return the SAME
     * segment instance (proving the values share the dictionary rather than being copied per row).
     */
    @Nested
    class DictionaryByteArrayWithNulls {

        @Test
        void sharesDictionaryEntriesAcrossRowsAndBatches() throws IOException {
            // Dictionary: ["alpha", "beta"]. Six rows, present pattern 1,0,1,1,0,1.
            // Indices for the four non-null rows: 0, 1, 0, 1 -> "alpha", "beta", "alpha", "beta".
            byte[][] dictValues = {bytes("alpha"), bytes("beta")};
            int[] indices = {0, 1, 0, 1};
            int[] defLevels = {1, 0, 1, 1, 0, 1};
            FetchedColumnChunk chunk = dictionaryEncodedByteArrayChunk(dictValues, indices, defLevels);

            BatchColumnReader reader = new BatchColumnReader(TestDecodeBuffers.ample(), chunk, optionalByteArrayLeaf());

            // First batch: rows 0..2 -> "alpha", null, "beta".
            ColumnVector batch1 = reader.readBatch(3, TestDecodeBuffers.batchBuffers());
            assertByteArraySlice(batch1, new byte[][] {bytes("alpha"), null, bytes("beta")});

            // Second batch: rows 3..5 -> "alpha", null, "beta".
            ColumnVector batch2 = reader.readBatch(3, TestDecodeBuffers.batchBuffers());
            assertByteArraySlice(batch2, new byte[][] {bytes("alpha"), null, bytes("beta")});

            assertThat(reader.hasMore()).isFalse();

            // The two "alpha" rows (batch1 row 0, batch2 row 0) reference the same dictionary entry instance.
            MemorySegment firstAlpha = ((BinaryVector) batch1).get(0);
            MemorySegment secondAlpha = ((BinaryVector) batch2).get(0);
            assertThat(firstAlpha).isSameAs(secondAlpha);
        }

        /**
         * Pins the placeholder-index invariant the reused pooled metadata segment depends on: the dictionary vectors
         * hand out the raw {@code dictionaryIndices()} sequence wholesale - the Arrow buffer codec serializes every
         * slot, null rows included - and {@code dictEntries[indices.get(row)]} is resolved from those slots without
         * consulting validity per slot, hence every null row's slot must hold the harmless placeholder index 0. Page 1
         * is all-valid and writes the highest dictionary index into every slot; if page 2's decode reused that backing
         * without the in-place spread zeroing null slots, page 2's null rows would still hold page 1's index and leak
         * the wrong dictionary entry.
         */
        @Test
        void dictionaryNullRowsKeepThePlaceholderIndexAcrossReusedPages() throws IOException {
            byte[][] dictValues = {bytes("alpha"), bytes("beta"), bytes("gamma")};
            // Page 1: four valid rows, every one referencing the highest dictionary index (2 -> "gamma").
            int[] page1DefLevels = {1, 1, 1, 1};
            int[] page1Indices = {2, 2, 2, 2};
            // Page 2: nulls at rows 1 and 2; the valid rows reference entries 0 and 1.
            int[] page2DefLevels = {1, 0, 0, 1};
            int[] page2Indices = {0, 1};
            FetchedColumnChunk chunk = dictionaryEncodedByteArrayChunkTwoPages(
                    dictValues, page1DefLevels, page1Indices, page2DefLevels, page2Indices);

            BatchColumnReader reader = new BatchColumnReader(TestDecodeBuffers.ample(), chunk, optionalByteArrayLeaf());

            ColumnVector batch1 = reader.readBatch(4, TestDecodeBuffers.batchBuffers());
            assertByteArraySlice(batch1, new byte[][] {bytes("gamma"), bytes("gamma"), bytes("gamma"), bytes("gamma")});

            ColumnVector batch2 = reader.readBatch(4, TestDecodeBuffers.batchBuffers());
            assertByteArraySlice(batch2, new byte[][] {bytes("alpha"), null, null, bytes("beta")});
            assertThat(reader.hasMore()).isFalse();

            // The null rows' slots in the raw index array hold the placeholder index 0 and resolve the placeholder
            // entry ("alpha"), not page 1's lingering index 2 ("gamma").
            BinaryVector binary = (BinaryVector) batch2;
            IntSequence rawIndices = binary.dictionaryIndices();
            MemorySegment[] entries = binary.dictionaryEntries();
            assertThat(rawIndices.get(1)).isZero();
            assertThat(rawIndices.get(2)).isZero();
            assertThat(entries[rawIndices.get(1)].toArray(JAVA_BYTE)).containsExactly(bytes("alpha"));
            assertThat(entries[rawIndices.get(2)].toArray(JAVA_BYTE)).containsExactly(bytes("alpha"));
        }

        /**
         * Pins the zero placeholder for null rows' index slots in the pooled page-metadata segment. A fresh heap array
         * was zero-filled by the JVM; a pooled segment may hold stale bytes from a previous borrower. Same two-dirty-
         * block seeding as the fixed-width tests: the page's small validity-bitmap acquire runs first and consumes one
         * dirty block, leaving the other, still dirty, for the indices acquire. A stale 0xFFFFFFFF slot would resolve
         * {@code dictEntries[indices.get(row)]} out of bounds; every null row's slot must read the placeholder index 0.
         */
        @Test
        void dictionaryNullRowSlotsReadThePlaceholderIndexNotStalePoolBytes() throws IOException {
            int rows = 4096; // 16KB of i32 index slots
            SegmentPool pool = SegmentPool.create();
            seedTwoDirtyBlocks(pool, (long) rows * Integer.BYTES);
            assertThat(pool.stats().freeSegments())
                    .as("both dirty blocks must be retained or this test pins nothing")
                    .isEqualTo(2);

            byte[][] dictValues = {bytes("alpha"), bytes("beta"), bytes("gamma")};
            int[] defLevels = new int[rows];
            int[] expectedIndexPerRow = new int[rows];
            int[] nonNullIndices = new int[rows - 2];
            int v = 0;
            for (int row = 0; row < rows; row++) {
                boolean isNull = (row == 100 || row == 4000);
                defLevels[row] = isNull ? 0 : 1;
                if (!isNull) {
                    expectedIndexPerRow[row] = v % dictValues.length;
                    nonNullIndices[v] = v % dictValues.length;
                    v++;
                }
            }
            FetchedColumnChunk chunk = dictionaryEncodedByteArrayChunk(dictValues, nonNullIndices, defLevels);
            BatchColumnReader reader =
                    new BatchColumnReader(TestDecodeBuffers.ample(pool), chunk, optionalByteArrayLeaf());

            BinaryVector binary = (BinaryVector) reader.readBatch(rows, TestDecodeBuffers.batchBuffers());

            IntSequence rawIndices = binary.dictionaryIndices();
            MemorySegment[] entries = binary.dictionaryEntries();
            assertThat(binary.validity().isValid(100)).isFalse();
            assertThat(binary.validity().isValid(4000)).isFalse();
            assertThat(rawIndices.get(100))
                    .as("null slot reads the placeholder index")
                    .isZero();
            assertThat(rawIndices.get(4000))
                    .as("null slot reads the placeholder index")
                    .isZero();
            assertThat(entries[rawIndices.get(100)].toArray(JAVA_BYTE)).containsExactly(bytes("alpha"));
            assertThat(entries[rawIndices.get(4000)].toArray(JAVA_BYTE)).containsExactly(bytes("alpha"));
            for (int row : new int[] {0, 99, 101, 3999, 4001, 4095}) {
                assertThat(binary.get(row).toArray(JAVA_BYTE))
                        .as("valid row %d resolves its own entry; the in-place spread must not shift it", row)
                        .containsExactly(dictValues[expectedIndexPerRow[row]]);
            }
        }

        /**
         * Pins the layout of a page holding nulls only. It holds no index and reads no dictionary, which makes its
         * vector a consolidated one with a zero offset for each row.
         */
        @Test
        void allNullPageReadsConsolidated() throws IOException {
            int rows = 8;
            byte[][] dictValues = {bytes("alpha"), bytes("beta")};
            int[] defLevels = new int[rows]; // all zero: every row is null
            FetchedColumnChunk chunk = dictionaryEncodedByteArrayChunk(dictValues, new int[0], defLevels);
            BatchColumnReader reader = new BatchColumnReader(TestDecodeBuffers.ample(), chunk, optionalByteArrayLeaf());

            BinaryVector binary = (BinaryVector) reader.readBatch(rows, TestDecodeBuffers.batchBuffers());

            assertThat(binary.validity().cardinality()).isZero();
            assertThat(binary.isDictionary()).isFalse();
            IntSequence offsets = binary.consolidatedOffsets();
            for (int row = 0; row <= rows; row++) {
                assertThat(offsets.get(row)).as("offset of row %d", row).isZero();
            }
        }

        private void assertByteArraySlice(ColumnVector vec, byte[][] expected) {
            assertThat(vec).isInstanceOf(BinaryVector.class);
            assertThat(vec.size()).isEqualTo(expected.length);
            BinaryVector binary = (BinaryVector) vec;
            for (int row = 0; row < expected.length; row++) {
                if (expected[row] == null) {
                    assertThat(binary.validity().isValid(row))
                            .as("row %d is null", row)
                            .isFalse();
                } else {
                    assertThat(binary.validity().isValid(row))
                            .as("row %d is present", row)
                            .isTrue();
                    assertThat(binary.get(row).toArray(JAVA_BYTE))
                            .as("row %d bytes", row)
                            .containsExactly(expected[row]);
                }
            }
        }
    }

    // --- on-demand value decode: a page stays compressed until a window reads it ---

    /** One column of each kind, with its rows on two pages. */
    static Stream<Arguments> onDemandColumns() throws IOException {
        return Stream.of(
                Arguments.of(
                        "INT32",
                        twoPageInt32Chunk(new int[] {1, 2, 3, 4}, new int[] {5, 6, 7, 8, 9}),
                        requiredInt32Leaf()),
                Arguments.of(
                        "INT64",
                        twoPageInt64Chunk(new long[] {1, 2, 3, 4}, new long[] {5, 6, 7, 8, 9}),
                        requiredLongLeaf()),
                Arguments.of(
                        "FLOAT",
                        twoPageFloatChunk(new float[] {1, 2, 3, 4}, new float[] {5, 6, 7, 8, 9}),
                        requiredFloatLeaf()),
                Arguments.of(
                        "DOUBLE",
                        twoPageDoubleChunk(new double[] {1, 2, 3, 4}, new double[] {5, 6, 7, 8, 9}),
                        requiredDoubleLeaf()),
                Arguments.of("nullable INT32", nullableInt32Chunk(), optionalInt32Leaf()),
                Arguments.of("PLAIN BYTE_ARRAY", plainByteArrayChunk(), optionalByteArrayLeaf()),
                Arguments.of("dictionary BYTE_ARRAY", dictionaryByteArrayChunk(), optionalByteArrayLeaf()));
    }

    private static FetchedColumnChunk nullableInt32Chunk() throws IOException {
        byte[] page1 = nullableInt32Page(new int[] {1, 0, 1, 0}, new int[] {10, 20});
        byte[] page2 = nullableInt32Page(new int[] {0, 1, 1, 0, 1}, new int[] {30, 40, 50});
        return twoPageChunk(page1, page2, /*totalValues*/ 9, /*maxDef*/ 1);
    }

    private static FetchedColumnChunk plainByteArrayChunk() throws IOException {
        byte[][] page1 = {bytes("a"), null, bytes("ccc"), bytes("dd")};
        byte[][] page2 = {null, bytes("eeeee"), bytes(""), null, bytes("g")};
        return twoPageNullableByteArrayChunk(page1, page2);
    }

    private static FetchedColumnChunk dictionaryByteArrayChunk() throws IOException {
        byte[][] dictValues = {bytes("alpha"), bytes("beta"), bytes("gamma")};
        int[] page1DefLevels = {1, 0, 1, 1};
        int[] page1Indices = {0, 1, 2};
        int[] page2DefLevels = {1, 1, 0, 1, 0};
        int[] page2Indices = {2, 1, 0};
        return dictionaryEncodedByteArrayChunkTwoPages(
                dictValues, page1DefLevels, page1Indices, page2DefLevels, page2Indices);
    }

    /** Each column under windows of at most 1, 2, 3 and all the rows of a page, opening on a read and on a skip. */
    static Stream<Arguments> onDemandWalks() throws IOException {
        return onDemandColumns().flatMap(BatchColumnReaderTest::walksOf);
    }

    private static Stream<Arguments> walksOf(Arguments column) {
        Object[] kindChunkAndLeaf = column.get();
        List<Arguments> walks = new ArrayList<>();
        for (int longestWindow : new int[] {1, 2, 3, Integer.MAX_VALUE}) {
            for (boolean opensOnARead : new boolean[] {true, false}) {
                walks.add(Arguments.of(
                        kindChunkAndLeaf[0], kindChunkAndLeaf[1], kindChunkAndLeaf[2], longestWindow, opensOnARead));
            }
        }
        return walks.stream();
    }

    @ParameterizedTest(name = "{0}, windows of {3}, opening on a read {4}")
    @MethodSource("onDemandWalks")
    void onDemandMatchesEager(
            String kind, FetchedColumnChunk chunk, SchemaNode.Primitive leaf, int longestWindow, boolean opensOnARead) {
        List<Object> expected = eagerRows(chunk, leaf);
        BatchColumnReader reader = onDemandReader(chunk, leaf);
        BatchBuffers buffers = TestDecodeBuffers.batchBuffers();
        boolean read = opensOnARead;
        int firstRow = 0;
        while (reader.hasMore()) {
            int windowRows = Math.min(longestWindow, reader.logicalRowsRemainingInCurrentPage());
            if (read) {
                List<Object> rows = rowsOf(reader.readBatch(windowRows, buffers));
                List<Object> expectedRows = expected.subList(firstRow, firstRow + windowRows);
                assertThat(rows).as("%s rows from %d", kind, firstRow).isEqualTo(expectedRows);
            } else {
                reader.skipWindow(windowRows);
            }
            read = !read;
            firstRow += windowRows;
        }
        assertThat(firstRow).isEqualTo(expected.size());
    }

    // One row in three reads. The steps between them cross held pages and loaded pages.
    @ParameterizedTest(name = "{0}")
    @MethodSource("onDemandColumns")
    void spanningRowsMatchEager(String kind, FetchedColumnChunk chunk, SchemaNode.Primitive leaf) {
        List<Object> expected = eagerRows(chunk, leaf);
        BatchColumnReader reader = onDemandReader(chunk, leaf);
        BatchBuffers buffers = TestDecodeBuffers.batchBuffers();
        for (int row = 0; row < expected.size(); row++) {
            if (row % 3 == 1) {
                BatchColumnReader.SpanningRow spanningRow = reader.readSpanningRow(buffers);
                List<Object> rows = rowsOf(spanningRow.vector());
                assertThat(rows).as("%s row %d", kind, row).isEqualTo(expected.subList(row, row + 1));
            } else {
                reader.skipSpanningRow();
            }
        }
        assertThat(reader.hasMore()).isFalse();
    }

    @Test
    void deadPageStaysCompressed() throws IOException {
        FetchedColumnChunk chunk = plainInt32Chunk(new int[] {1, 2}, new int[] {3, 4}, new int[] {5, 6});
        BatchColumnReader reader = onDemandReader(chunk, requiredInt32Leaf());

        reader.skipWindow(reader.logicalRowsRemainingInCurrentPage());

        assertThat(reader.decodedDataPageCount()).isZero();
        assertThat(reader.skippedDataPageCount()).isEqualTo(1);
    }

    @Test
    void rowStepsDiscardAHeldPage() throws IOException {
        FetchedColumnChunk chunk = plainInt32Chunk(new int[] {1, 2}, new int[] {3, 4});
        BatchColumnReader reader = onDemandReader(chunk, requiredInt32Leaf());

        reader.skipSpanningRow();
        reader.skipSpanningRow();

        assertThat(reader.decodedDataPageCount()).isZero();
        assertThat(reader.skippedDataPageCount()).isEqualTo(1);
        ColumnVector rows = reader.readBatch(2, TestDecodeBuffers.batchBuffers());
        assertThat(rowsOf(rows)).containsExactly(3, 4);
    }

    @Test
    void readResumesPastSkippedRows() throws IOException {
        FetchedColumnChunk chunk = singlePageInt32Chunk(new int[] {0, 1, 2, 3, 4, 5, 6}, /*maxDef*/ 0);
        BatchColumnReader reader = onDemandReader(chunk, requiredInt32Leaf());

        reader.skipWindow(3);
        ColumnVector rows = reader.readBatch(2, TestDecodeBuffers.batchBuffers());

        assertThat(rowsOf(rows)).containsExactly(3, 4);
        assertThat(reader.logicalRowsRemainingInCurrentPage()).isEqualTo(2);
    }

    // The eager fast path of a flat column keeps no definition levels, and a loaded page takes it.
    @Test
    void nullsSurviveAHeldPage() throws IOException {
        byte[] page = nullableInt32Page(new int[] {1, 0, 1, 0, 0, 1}, new int[] {10, 20, 30});
        FetchedColumnChunk chunk = heapChunk(PATH, page, /*numValues*/ 6, /*maxRep*/ 0, /*maxDef*/ 1);
        BatchColumnReader reader = onDemandReader(chunk, optionalInt32Leaf());

        reader.skipWindow(2);
        ColumnVector rows = reader.readBatch(3, TestDecodeBuffers.batchBuffers());

        assertThat(rowsOf(rows)).containsExactly(20, null, null);
        assertThat(reader.currentPageDefLevels()).isNull();
    }

    @Test
    void zeroRowPageIsSteppedOver() throws IOException {
        FetchedColumnChunk chunk = plainInt32Chunk(new int[] {1, 2}, new int[] {}, new int[] {}, new int[] {3, 4, 5});
        BatchColumnReader reader = onDemandReader(chunk, requiredInt32Leaf());
        reader.skipWindow(reader.logicalRowsRemainingInCurrentPage());
        assertThat(reader.logicalRowsRemainingInCurrentPage()).isZero();

        reader.skipSpanningRow();

        assertThat(reader.decodedDataPageCount()).isZero();
        assertThat(reader.skippedDataPageCount()).isEqualTo(3);
        ColumnVector rows = reader.readBatch(2, TestDecodeBuffers.batchBuffers());
        assertThat(rowsOf(rows)).containsExactly(4, 5);
    }

    static Stream<Arguments> wrongV2RowCounts() {
        // each vector is the lane stepping over the page, and the rows stated by a page of three values
        return Stream.of(
                Arguments.of(ValueDecode.EAGER_ON_DEMAND, 2),
                Arguments.of(ValueDecode.EAGER_ON_DEMAND, 4),
                Arguments.of(ValueDecode.WINDOWED_MASK, 2),
                Arguments.of(ValueDecode.WINDOWED_MASK, 4));
    }

    // A step over the page decodes nothing, and a wrong count would shift the values of each later page.
    @ParameterizedTest
    @MethodSource("wrongV2RowCounts")
    void wrongV2RowCountFailsAStep(ValueDecode valueDecode, int statedRows) throws IOException {
        byte[] pages = concat(encodeV2Page(2, new int[] {1, 2}), encodeV2Page(statedRows, new int[] {3, 4, 5}));
        FetchedColumnChunk chunk = heapChunk(PATH, pages, /*numValues*/ 5, /*maxRep*/ 0, /*maxDef*/ 0);
        BatchColumnReader reader =
                new BatchColumnReader(TestDecodeBuffers.ample(), chunk, requiredInt32Leaf(), null, null, valueDecode);
        reader.skipWindow(reader.logicalRowsRemainingInCurrentPage());

        assertThatThrownBy(reader::logicalRowsRemainingInCurrentPage)
                .isInstanceOf(MalformedFileException.class)
                .hasMessageContaining("Column val V2 data page 1 counts " + statedRows + " rows and 3 values");
        assertThat(reader.decodedDataPageCount()).isZero();
    }

    @Test
    void rowStepPeeksWithNoPageHeld() throws IOException {
        FetchedColumnChunk chunk = plainInt32Chunk(new int[] {}, new int[] {1, 2});
        BatchColumnReader reader = onDemandReader(chunk, requiredInt32Leaf());

        reader.skipSpanningRow();

        assertThat(reader.skippedDataPageCount()).isEqualTo(1);
        ColumnVector rows = reader.readBatch(1, TestDecodeBuffers.batchBuffers());
        assertThat(rowsOf(rows)).containsExactly(2);
    }

    @Test
    void zeroRowPageIsReadThrough() throws IOException {
        FetchedColumnChunk chunk = plainInt32Chunk(new int[] {}, new int[] {1, 2});
        BatchColumnReader reader = onDemandReader(chunk, requiredInt32Leaf());
        assertThat(reader.logicalRowsRemainingInCurrentPage()).isZero();

        BatchColumnReader.SpanningRow row = reader.readSpanningRow(TestDecodeBuffers.batchBuffers());
        ColumnVector rows = row.vector();

        assertThat(rowsOf(rows)).containsExactly(1);
        assertThat(reader.logicalRowsRemainingInCurrentPage()).isEqualTo(1);
    }

    // A level accessor loads the page held compressed, whatever its row count.
    @Test
    void loadedZeroRowPageTakesNoStep() throws IOException {
        FetchedColumnChunk chunk = plainInt32Chunk(new int[] {}, new int[] {1, 2});
        BatchColumnReader reader = onDemandReader(chunk, requiredInt32Leaf());
        reader.logicalRowsRemainingInCurrentPage();
        reader.currentPageDefLevels();

        reader.skipSpanningRow();

        ColumnVector rows = reader.readBatch(1, TestDecodeBuffers.batchBuffers());
        assertThat(rowsOf(rows)).containsExactly(2);
    }

    @Test
    void rowStepPastTheLastPageFails() throws IOException {
        FetchedColumnChunk chunk = plainInt32Chunk(new int[] {1}, new int[] {});
        BatchColumnReader reader = onDemandReader(chunk, requiredInt32Leaf());
        reader.skipSpanningRow();

        assertThatThrownBy(reader::skipSpanningRow)
                .isInstanceOf(MalformedFileException.class)
                .hasMessageContaining("Column val exhausted page stream after 1 values");
    }

    @Test
    void eagerRejectsSkipWindow() throws IOException {
        FetchedColumnChunk chunk = singlePageInt32Chunk(new int[] {1, 2}, /*maxDef*/ 0);
        BatchColumnReader reader = new BatchColumnReader(TestDecodeBuffers.ample(), chunk, requiredInt32Leaf());

        assertThatThrownBy(() -> reader.skipWindow(1)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void onlyOnDemandStepsOverARow() throws IOException {
        FetchedColumnChunk chunk = singlePageInt32Chunk(new int[] {1, 2}, /*maxDef*/ 0);
        BatchColumnReader eager = new BatchColumnReader(TestDecodeBuffers.ample(), chunk, requiredInt32Leaf());
        BatchColumnReader windowed = new BatchColumnReader(
                TestDecodeBuffers.ample(), chunk, requiredInt32Leaf(), null, null, ValueDecode.WINDOWED_MASK);

        assertThatThrownBy(eager::skipSpanningRow).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(windowed::skipSpanningRow).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void onDemandRejectsARepeatedLeaf() throws IOException {
        byte[] page = repeatedInt32Page(new int[] {0, 1}, new int[] {1, 2});
        FetchedColumnChunk chunk = heapChunk(PATH, page, /*numValues*/ 2, /*maxRep*/ 1, /*maxDef*/ 1);
        SchemaNode.Primitive leaf = repeatedInt32Leaf();

        assertThatThrownBy(() -> onDemandReader(chunk, leaf))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("column val is repeated");
    }

    private static BatchColumnReader onDemandReader(FetchedColumnChunk chunk, SchemaNode.Primitive leaf) {
        return new BatchColumnReader(TestDecodeBuffers.ample(), chunk, leaf, null, null, ValueDecode.EAGER_ON_DEMAND);
    }

    /** The rows of {@code chunk} as an eager reader reads them, the oracle of the on-demand walks. */
    private static List<Object> eagerRows(FetchedColumnChunk chunk, SchemaNode.Primitive leaf) {
        BatchColumnReader reader = new BatchColumnReader(TestDecodeBuffers.ample(), chunk, leaf);
        BatchBuffers buffers = TestDecodeBuffers.batchBuffers();
        List<Object> rows = new ArrayList<>();
        while (reader.hasMore()) {
            ColumnVector batch = reader.readBatch(Integer.MAX_VALUE, buffers);
            rows.addAll(rowsOf(batch));
        }
        return rows;
    }

    /** The rows of {@code vector} with null at the null rows, and each binary value as text to compare by content. */
    private static List<Object> rowsOf(ColumnVector vector) {
        List<Object> rows = new ArrayList<>(vector.size());
        for (int row = 0; row < vector.size(); row++) {
            Object value = vector.get(row);
            if (value instanceof MemorySegment binary) {
                value = new String(binary.toArray(JAVA_BYTE), StandardCharsets.UTF_8);
            }
            rows.add(value);
        }
        return rows;
    }

    // --- a dictionary decoded when a page uses it ---

    @Test
    void plainPageDecodesNoDictionary() throws IOException {
        SchemaNode.Primitive leaf = requiredInt32Leaf();
        FetchedColumnChunk plainPages = plainInt32Chunk(new int[] {1, 2}, new int[] {3});
        FetchedColumnChunk chunk = withCorruptDictionary(plainPages, PrimitiveKind.INT32);

        assertThat(eagerRows(chunk, leaf)).containsExactly(1, 2, 3);
        assertThat(windowedRows(chunk, leaf)).containsExactly(1, 2, 3);
    }

    @Test
    void dictionaryDecodesOnFirstUse() throws IOException {
        byte[] plainPage =
                encodeV1Page(2, encodeInt32sLittleEndian(new int[] {1, 2}), org.apache.parquet.format.Encoding.PLAIN);
        byte[] indexPage = encodeV1Page(
                2,
                encodeRleDictionaryIndexPage(new int[] {0, 1}, 2),
                org.apache.parquet.format.Encoding.RLE_DICTIONARY);
        FetchedColumnChunk pages = heapChunk(PATH, concat(plainPage, indexPage), 4, /*maxRep*/ 0, /*maxDef*/ 0);
        FetchedColumnChunk chunk = withCorruptDictionary(pages, PrimitiveKind.INT32);
        SegmentPool pool = SegmentPool.create();
        BatchColumnReader reader = new BatchColumnReader(TestDecodeBuffers.ample(pool), chunk, requiredInt32Leaf());
        BatchBuffers buffers = TestDecodeBuffers.batchBuffers();

        ColumnVector plainRows = reader.readBatch(2, buffers);

        assertThat(rowsOf(plainRows)).containsExactly(1, 2);
        assertThatThrownBy(() -> reader.readBatch(2, buffers))
                .isInstanceOf(MalformedFileException.class)
                .hasMessageContaining("Column val data page 1")
                .hasMessageContaining("dictionary page");
        reader.close();
        assertThat(pool.stats().outstandingBorrows()).isZero();
    }

    @ParameterizedTest
    @EnumSource(
            value = ValueDecode.class,
            names = {"EAGER_ON_DEMAND", "WINDOWED_MASK"})
    void heldDeadPageNeedsNoDictionary(ValueDecode valueDecode) throws IOException {
        FetchedColumnChunk indexPage = dictionaryEncodedInt32Chunk(new int[] {100, 200}, new int[] {0, 1});
        FetchedColumnChunk chunk = withCorruptDictionary(indexPage, PrimitiveKind.INT32);
        BatchColumnReader reader =
                new BatchColumnReader(TestDecodeBuffers.ample(), chunk, requiredInt32Leaf(), null, null, valueDecode);

        reader.skipWindow(reader.logicalRowsRemainingInCurrentPage());

        assertThat(reader.skippedDataPageCount()).isEqualTo(1);
        assertThat(reader.hasMore()).isFalse();
    }

    @ParameterizedTest
    @EnumSource(PrimitiveKind.class)
    void allNullPageNeedsNoDictionary(PrimitiveKind kind) throws IOException {
        int rows = 5;
        SchemaNode.Primitive leaf = optionalLeaf(kind);
        FetchedColumnChunk chunk = withCorruptDictionary(allNullIndexPage(rows), kind);

        List<Object> nulls = Collections.nCopies(rows, null);
        List<Object> nullsUnderTheMask = Collections.nCopies(rows - 2, null);

        assertThat(eagerRows(chunk, leaf)).isEqualTo(nulls);
        assertThat(windowedRows(chunk, leaf)).isEqualTo(nulls);
        assertThat(maskedRows(chunk, leaf, rows, ValueDecode.EAGER)).isEqualTo(nullsUnderTheMask);
        assertThat(maskedRows(chunk, leaf, rows, ValueDecode.WINDOWED_MASK)).isEqualTo(nullsUnderTheMask);
    }

    // The shape of a vector follows its page inside one reader, and the dictionary outlasts the page of nulls.
    @ParameterizedTest
    @EnumSource(
            value = ValueDecode.class,
            names = {"EAGER", "WINDOWED_MASK"})
    void nullPageBetweenIndexPages(ValueDecode valueDecode) throws IOException {
        byte[][] dictValues = {bytes("alpha"), bytes("beta")};
        byte[] firstPage = encodeDictionaryByteArrayPage(2, new int[] {1, 1}, new int[] {0, 1});
        byte[] nullPage = encodeDictionaryByteArrayPage(2, new int[] {0, 0}, new int[0]);
        byte[] lastPage = encodeDictionaryByteArrayPage(2, new int[] {1, 1}, new int[] {1, 0});
        byte[] chunkBuffer = concat(concat(firstPage, nullPage), lastPage);
        FetchedColumnChunk chunk = dictionaryByteArrayChunk(dictValues, chunkBuffer, 6);
        SegmentPool pool = SegmentPool.create();
        DecodeBufferAllocator noDecodeRoom = TestDecodeBuffers.spilling(pool);
        BatchColumnReader reader =
                new BatchColumnReader(noDecodeRoom, chunk, optionalByteArrayLeaf(), null, null, valueDecode);

        List<ColumnVector> pages = pageVectors(reader, valueDecode);
        reader.close();

        assertThat(pages)
                .extracting(page -> ((BinaryVector) page).isDictionary())
                .containsExactly(true, false, true);
        assertThat(rowsOfAll(pages)).containsExactly("alpha", "beta", null, null, "beta", "alpha");
        assertThat(pool.stats().totalBorrows())
                .as("one borrow per data page and one for the dictionary of all three")
                .isEqualTo(4L);
    }

    @Test
    void indexPageNeedsADictionaryPage() throws IOException {
        byte[] intPage = encodeV1Page(
                2,
                encodeRleDictionaryIndexPage(new int[] {0, 1}, 2),
                org.apache.parquet.format.Encoding.RLE_DICTIONARY);
        byte[] binaryPage = encodeDictionaryByteArrayPage(2, new int[] {1}, new int[] {0});
        FetchedColumnChunk ints = heapChunk(PATH, intPage, 2, /*maxRep*/ 0, /*maxDef*/ 0);
        FetchedColumnChunk binaries = heapChunk(PATH, binaryPage, 1, /*maxRep*/ 0, /*maxDef*/ 1);
        DecodeBufferAllocator allocator = TestDecodeBuffers.ample();
        BatchColumnReader intReader = new BatchColumnReader(allocator, ints, requiredInt32Leaf());
        BatchColumnReader binaryReader = new BatchColumnReader(allocator, binaries, optionalByteArrayLeaf());
        BatchBuffers buffers = TestDecodeBuffers.batchBuffers();

        assertThatThrownBy(() -> intReader.readBatch(2, buffers))
                .isInstanceOf(MalformedFileException.class)
                .hasMessageContaining("needs a dictionary page");
        assertThatThrownBy(() -> binaryReader.readBatch(1, buffers))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("none supplied for column val");
    }

    // The decode budget has no room. The decode valve maps files, and the pool counts the page arenas alone.
    @Test
    void dictionaryBorrowsFromThePool() throws IOException {
        SegmentPool pool = SegmentPool.create();
        DecodeBufferAllocator noDecodeRoom = TestDecodeBuffers.spilling(pool);
        BatchColumnReader reader =
                new BatchColumnReader(noDecodeRoom, dictionaryByteArrayChunk(), optionalByteArrayLeaf());
        BatchBuffers buffers = TestDecodeBuffers.batchBuffers();

        reader.readBatch(Integer.MAX_VALUE, buffers);
        reader.readBatch(Integer.MAX_VALUE, buffers);
        reader.close();

        assertThat(pool.stats().totalBorrows())
                .as("one borrow per data page and one for the dictionary of both")
                .isEqualTo(3L);
        assertThat(pool.stats().outstandingBorrows()).isZero();
    }

    // The decoded dictionary holds heap copies: reusing the pool's buffers cannot change its values.
    @Test
    void dictionaryOutlivesItsBuffer() {
        Path file = CorpusFixtures.parquetTestingData().resolve("alltypes_dictionary.parquet");
        SegmentPool pool = SegmentPool.create();
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            BinaryVector strings = readStringColumn(source, pool);
            assertThat(strings.isDictionary())
                    .as("the fixture is dictionary-encoded")
                    .isTrue();
            byte[] first = strings.get(0).toArray(JAVA_BYTE);
            assertThat(first).isNotEmpty();

            scribbleOverFreeBuffers(pool);

            assertThat(strings.get(0).toArray(JAVA_BYTE))
                    .as("the dictionary does not read from the returned buffer")
                    .isEqualTo(first);
        } finally {
            pool.close();
        }
    }

    /** The string column of the first row group. The reader and the fetch behind it are closed when this returns. */
    private static BinaryVector readStringColumn(ByteRangeSource source, SegmentPool pool) {
        FileMetaData footer = ParquetFormat.readFooter(source);
        ParquetSchema schema = SchemaBuilder.build(footer.schema());
        RowGroupSurvivor survivor =
                RowGroupSurvivor.full(TestRowGroupChunks.of(footer, 0, schema, TestRowGroupChunks.reading(source)));
        RowGroupFetcher fetcher = TestFetchers.over(source, schema, schema, pool);
        FetchPlan plan = fetcher.planFor(survivor, Optional.empty());
        ColumnPath stringColumn = ColumnPath.of("string_col");
        SchemaNode.Primitive leaf =
                (SchemaNode.Primitive) schema.find(stringColumn).orElseThrow();
        try (RowGroupFetch fetch = fetcher.fetch(survivor, plan, BudgetReservation.NONE)) {
            FetchedColumnChunk chunk = fetch.columns().get(schema.leafColumns().indexOf(stringColumn));
            BatchColumnReader reader = new BatchColumnReader(TestDecodeBuffers.ample(pool), chunk, leaf);
            ColumnVector strings = reader.readBatch(Integer.MAX_VALUE, TestDecodeBuffers.batchBuffers());
            reader.close();
            return (BinaryVector) strings;
        }
    }

    /** Borrows every free buffer of the pool, overwrites it and returns it. */
    private static void scribbleOverFreeBuffers(SegmentPool pool) {
        int free = pool.stats().freeSegments();
        assertThat(free).as("the dictionary buffers went back to the pool").isPositive();
        List<SegmentPool.Pooled> borrowed = new ArrayList<>();
        for (int i = 0; i < free; i++) {
            SegmentPool.Pooled buffer = pool.borrow(8192);
            buffer.segment().fill((byte) 0xFF);
            borrowed.add(buffer);
        }
        borrowed.forEach(SegmentPool.Pooled::close);
    }

    /**
     * {@code chunk} with a dictionary page whose decode fails. The page declares more values than its bytes hold, and a
     * FIXED_LEN_BYTE_ARRAY page fails earlier, for its missing type length.
     */
    private static FetchedColumnChunk withCorruptDictionary(FetchedColumnChunk chunk, PrimitiveKind kind) {
        return new FetchedColumnChunk(
                chunk.path(),
                chunk.codec(),
                chunk.numValues(),
                chunk.maxRepetitionLevel(),
                chunk.maxDefinitionLevel(),
                chunk.dataPageRuns(),
                Optional.of(dictionaryPageOf(kind, new byte[3], 64)));
    }

    /** One dictionary-encoded page of an optional column holding {@code rows} nulls. */
    private static FetchedColumnChunk allNullIndexPage(int rows) throws IOException {
        byte[] defLevels = rleEncodeBits(new int[rows], /*maxLevel*/ 1);
        byte[] noIndex = encodeRleDictionaryIndexPage(new int[0], 2);
        byte[] payload = buildV1PayloadWithDefLevels(defLevels, noIndex);
        byte[] page = encodeV1Page(rows, payload, org.apache.parquet.format.Encoding.RLE_DICTIONARY);
        return heapChunk(PATH, page, rows, /*maxRep*/ 0, /*maxDef*/ 1);
    }

    /** The rows of {@code chunk} as the windowed lane reads them, one window per page, each row kept. */
    private static List<Object> windowedRows(FetchedColumnChunk chunk, SchemaNode.Primitive leaf) {
        BatchColumnReader reader =
                new BatchColumnReader(TestDecodeBuffers.ample(), chunk, leaf, null, null, ValueDecode.WINDOWED_MASK);
        List<ColumnVector> pages = pageVectors(reader, ValueDecode.WINDOWED_MASK);
        return rowsOfAll(pages);
    }

    /** The rows of the one page of {@code chunk} kept by a row mask dropping the first and the last of them. */
    private static List<Object> maskedRows(
            FetchedColumnChunk chunk, SchemaNode.Primitive leaf, int pageRows, ValueDecode valueDecode) {
        RowRanges mask = new RowRanges(List.of(new Range(1, pageRows - 2)));
        PageIndex onePage = TestPageIndexes.pages(pageRows, 0);
        BatchColumnReader reader =
                new BatchColumnReader(TestDecodeBuffers.ample(), chunk, leaf, mask, onePage, valueDecode);
        List<ColumnVector> pages = pageVectors(reader, valueDecode);
        return rowsOfAll(pages);
    }

    /** One vector per page of {@code reader}, read through its lane with each row kept. */
    private static List<ColumnVector> pageVectors(BatchColumnReader reader, ValueDecode valueDecode) {
        BatchBuffers buffers = TestDecodeBuffers.batchBuffers();
        List<ColumnVector> pages = new ArrayList<>();
        while (reader.hasMore()) {
            if (valueDecode == ValueDecode.EAGER) {
                pages.add(reader.readBatch(Integer.MAX_VALUE, buffers));
            } else {
                pages.add(wholeWindow(reader, buffers));
            }
        }
        return pages;
    }

    /** The rows left in the current page of a windowed reader, each row kept. */
    private static ColumnVector wholeWindow(BatchColumnReader reader, BatchBuffers buffers) {
        int windowRows = reader.logicalRowsRemainingInCurrentPage();
        BitSet eachRow = new BitSet(windowRows);
        eachRow.set(0, windowRows);
        return reader.readMaskedWindow(windowRows, eachRow, buffers).vector();
    }

    private static List<Object> rowsOfAll(List<ColumnVector> vectors) {
        List<Object> rows = new ArrayList<>();
        for (ColumnVector vector : vectors) {
            rows.addAll(rowsOf(vector));
        }
        return rows;
    }

    private static SchemaNode.Primitive optionalLeaf(PrimitiveKind kind) {
        OptionalInt typeLength = kind == PrimitiveKind.FIXED_LEN_BYTE_ARRAY ? OptionalInt.of(4) : OptionalInt.empty();
        return new SchemaNode.Primitive("val", Repetition.OPTIONAL, kind, typeLength, Optional.empty(), -1);
    }

    // --- malformed pages ---

    @Test
    void pageDeclaringMoreValuesThanItsChunkIsAFormatError() throws IOException {
        byte[] page = encodeV1Page(
                4, encodeInt32sLittleEndian(new int[] {1, 2, 3, 4}), org.apache.parquet.format.Encoding.PLAIN);
        FetchedColumnChunk chunk = heapChunk(PATH, page, /*numValues*/ 2, /*maxRep*/ 0, /*maxDef*/ 0);
        BatchColumnReader reader = new BatchColumnReader(TestDecodeBuffers.ample(), chunk, requiredInt32Leaf());

        BatchBuffers buffers = TestDecodeBuffers.batchBuffers();
        assertThatThrownBy(() -> reader.readBatch(10, buffers))
                .isInstanceOf(MalformedFileException.class)
                .hasMessageContaining("Column val data page 0 declares 4 values but its column chunk holds 2");
    }

    @Test
    void requiredPageShortOfItsValueBytesFailsNamingTheColumnAndPage() throws IOException {
        byte[] page = encodeV1Page(
                4, encodeInt32sLittleEndian(new int[] {1, 2, 3}), org.apache.parquet.format.Encoding.PLAIN);
        FetchedColumnChunk chunk = heapChunk(PATH, page, /*numValues*/ 4, /*maxRep*/ 0, /*maxDef*/ 0);
        BatchColumnReader reader = new BatchColumnReader(TestDecodeBuffers.ample(), chunk, requiredInt32Leaf());

        BatchBuffers buffers = TestDecodeBuffers.batchBuffers();
        assertThatThrownBy(() -> reader.readBatch(10, buffers))
                .isInstanceOf(MalformedFileException.class)
                .hasMessage("Column val data page 0: 4 INT32 values need 16 bytes but the page holds 12");
    }

    @Test
    void repeatedPageOpeningMidRowWithNoRowOpenIsAFormatError() throws IOException {
        // repetition levels [1, 0]: the first value continues a row that never started
        byte[] page = repeatedInt32Page(new int[] {1, 0}, new int[] {7, 8});
        FetchedColumnChunk chunk = heapChunk(PATH, page, /*numValues*/ 2, /*maxRep*/ 1, /*maxDef*/ 1);
        BatchColumnReader reader = new BatchColumnReader(TestDecodeBuffers.ample(), chunk, repeatedInt32Leaf());

        assertThatThrownBy(reader::logicalRowsRemainingInCurrentPage)
                .isInstanceOf(MalformedFileException.class)
                .hasMessageContaining("Column val data page 0 opens with repetition level 1 but no row is open");
    }

    @Test
    void repeatedPageContinuingTheRowOfThePageBeforeItReads() throws IOException {
        // page 0 opens row 0 with [0, 1]; page 1 continues it with [1], then starts row 1 with [0]
        byte[] page0 = repeatedInt32Page(new int[] {0, 1}, new int[] {1, 2});
        byte[] page1 = repeatedInt32Page(new int[] {1, 0}, new int[] {3, 4});
        FetchedColumnChunk chunk = heapChunk(PATH, concat(page0, page1), /*numValues*/ 4, /*maxRep*/ 1, /*maxDef*/ 1);
        BatchColumnReader reader = new BatchColumnReader(TestDecodeBuffers.ample(), chunk, repeatedInt32Leaf());

        BatchColumnReader.SpanningRow row = reader.readSpanningRow(TestDecodeBuffers.batchBuffers());

        assertThat(intsOf(row.repLevels())).containsExactly(0, 1, 1);
        assertThat(VectorArrays.toArray((IntVector) row.vector())).containsExactly(1, 2, 3);
    }

    /**
     * Encodes one V1 page of a {@code repeated int32} column: the repetition and definition level sections, each
     * length-prefixed, then the values. Each value is present, at definition level 1.
     */
    private static byte[] repeatedInt32Page(int[] repLevels, int[] values) throws IOException {
        int[] defLevels = new int[values.length];
        Arrays.fill(defLevels, 1);
        byte[] repBytes = rleEncodeBits(repLevels, /*maxLevel*/ 1);
        byte[] defBytes = rleEncodeBits(defLevels, /*maxLevel*/ 1);
        byte[] levelsAndValues = concat(
                lengthPrefixed(repBytes), buildV1PayloadWithDefLevels(defBytes, encodeInt32sLittleEndian(values)));
        return encodeV1Page(values.length, levelsAndValues, org.apache.parquet.format.Encoding.PLAIN);
    }

    private static byte[] lengthPrefixed(byte[] section) {
        ByteBuffer buf = ByteBuffer.allocate(4 + section.length).order(LITTLE_ENDIAN);
        buf.putInt(section.length);
        buf.put(section);
        return buf.array();
    }

    private static SchemaNode.Primitive repeatedInt32Leaf() {
        return new SchemaNode.Primitive(
                "val", Repetition.REPEATED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
    }

    // --- fixture helpers ---

    /**
     * Builds a {@link FetchedColumnChunk} for a nullable dictionary-encoded BYTE_ARRAY column in a single V1 page. The
     * V1 payload is a def-level section (length-prefixed) followed by the RLE-dictionary index page: one bit-width byte
     * then the bit-packed indexes for the non-null rows.
     *
     * @param dictValues the unique byte-array values held by the dictionary
     * @param indices the per-non-null-row index references into the dictionary
     * @param defLevels one def level per row (1 = present, 0 = null), one index per present row
     */
    private static FetchedColumnChunk dictionaryEncodedByteArrayChunk(
            byte[][] dictValues, int[] indices, int[] defLevels) throws IOException {
        byte[] chunkBuffer = encodeDictionaryByteArrayPage(dictValues.length, defLevels, indices);
        return dictionaryByteArrayChunk(dictValues, chunkBuffer, defLevels.length);
    }

    /**
     * Builds a {@link FetchedColumnChunk} with two nullable dictionary-encoded BYTE_ARRAY data pages concatenated in
     * one chunk buffer, both resolving through the same chunk-level dictionary. Same per-page layout as
     * {@link #dictionaryEncodedByteArrayChunk}.
     */
    private static FetchedColumnChunk dictionaryEncodedByteArrayChunkTwoPages(
            byte[][] dictValues, int[] defLevels1, int[] indices1, int[] defLevels2, int[] indices2)
            throws IOException {
        byte[] page1 = encodeDictionaryByteArrayPage(dictValues.length, defLevels1, indices1);
        byte[] page2 = encodeDictionaryByteArrayPage(dictValues.length, defLevels2, indices2);
        byte[] chunkBuffer = concat(page1, page2);
        return dictionaryByteArrayChunk(dictValues, chunkBuffer, defLevels1.length + defLevels2.length);
    }

    private static byte[] encodeDictionaryByteArrayPage(int dictionarySize, int[] defLevels, int[] indices)
            throws IOException {
        byte[] defLevelBytes = rleEncodeBits(defLevels, /*maxLevel*/ 1);
        byte[] indexPage = encodeRleDictionaryIndexPage(indices, dictionarySize);
        byte[] payload = buildV1PayloadWithDefLevels(defLevelBytes, indexPage);
        return encodeV1Page(defLevels.length, payload, org.apache.parquet.format.Encoding.RLE_DICTIONARY);
    }

    private static FetchedColumnChunk dictionaryByteArrayChunk(byte[][] dictValues, byte[] chunkBuffer, int numValues) {
        MemorySegment segment = MemorySegment.ofArray(chunkBuffer).asReadOnly();
        DictionaryPage dictionaryPage =
                dictionaryPageOf(PrimitiveKind.BYTE_ARRAY, encodePlainByteArrays(dictValues), dictValues.length);

        return new FetchedColumnChunk(
                PATH,
                CompressionCodec.UNCOMPRESSED,
                (long) numValues,
                /*maxRep*/ 0,
                /*maxDef*/ 1,
                List.of(new DataPageRun(segment, 0)),
                Optional.of(dictionaryPage));
    }

    /**
     * An uncompressed dictionary page over {@code plainEntries}, its entries in PLAIN encoding. A reader decodes it as
     * it decodes a dictionary page of a file.
     */
    static DictionaryPage dictionaryPageOf(PrimitiveKind kind, byte[] plainEntries, int valueCount) {
        MemorySegment payload = MemorySegment.ofArray(plainEntries).asReadOnly();
        return new DictionaryPage(
                payload, plainEntries.length, valueCount, Compression.uncompressed(), kind, OptionalInt.empty(), PATH);
    }

    /**
     * Builds a {@link FetchedColumnChunk} containing a single PLAIN INT32 page with no def/rep levels (required
     * column).
     */
    private static FetchedColumnChunk singlePageInt32Chunk(int[] values, int maxDef) throws IOException {
        byte[] valueBytes = encodeInt32sLittleEndian(values);
        // V1 payload for required column: no level prefix sections, just the value bytes.
        byte[] payload = valueBytes;
        byte[] chunkBuffer = encodeV1Page(values.length, payload, org.apache.parquet.format.Encoding.PLAIN);
        return heapChunk(PATH, chunkBuffer, values.length, /*maxRep*/ 0, maxDef);
    }

    /**
     * Builds a {@link FetchedColumnChunk} with two PLAIN DOUBLE pages concatenated in one chunk buffer (required
     * column, no def/rep levels). The value bytes are the little-endian DOUBLE layout the live-page slice copies
     * straight from.
     */
    private static FetchedColumnChunk twoPageDoubleChunk(double[] page1, double[] page2) throws IOException {
        byte[] payload1 = encodeDoublesLittleEndian(page1);
        byte[] payload2 = encodeDoublesLittleEndian(page2);
        byte[] page1Bytes = encodeV1Page(page1.length, payload1, org.apache.parquet.format.Encoding.PLAIN);
        byte[] page2Bytes = encodeV1Page(page2.length, payload2, org.apache.parquet.format.Encoding.PLAIN);
        byte[] chunkBuffer = concat(page1Bytes, page2Bytes);
        long totalValues = (long) page1.length + page2.length;
        return heapChunk(PATH, chunkBuffer, totalValues, /*maxRep*/ 0, /*maxDef*/ 0);
    }

    /** Builds a {@link FetchedColumnChunk} with one PLAIN INT32 page per array (required column, no def/rep levels). */
    private static FetchedColumnChunk plainInt32Chunk(int[]... pages) throws IOException {
        ByteArrayOutputStream chunkBuffer = new ByteArrayOutputStream();
        long totalValues = 0;
        for (int[] page : pages) {
            byte[] payload = encodeInt32sLittleEndian(page);
            chunkBuffer.write(encodeV1Page(page.length, payload, org.apache.parquet.format.Encoding.PLAIN));
            totalValues += page.length;
        }
        return heapChunk(PATH, chunkBuffer.toByteArray(), totalValues, /*maxRep*/ 0, /*maxDef*/ 0);
    }

    /** Builds a {@link FetchedColumnChunk} with two PLAIN INT32 pages concatenated in one chunk buffer. */
    private static FetchedColumnChunk twoPageInt32Chunk(int[] page1, int[] page2) throws IOException {
        byte[] payload1 = encodeInt32sLittleEndian(page1);
        byte[] payload2 = encodeInt32sLittleEndian(page2);
        byte[] page1Bytes = encodeV1Page(page1.length, payload1, org.apache.parquet.format.Encoding.PLAIN);
        byte[] page2Bytes = encodeV1Page(page2.length, payload2, org.apache.parquet.format.Encoding.PLAIN);
        byte[] chunkBuffer = concat(page1Bytes, page2Bytes);
        long totalValues = (long) page1.length + page2.length;
        return heapChunk(PATH, chunkBuffer, totalValues, /*maxRep*/ 0, /*maxDef*/ 0);
    }

    /**
     * Builds a {@link FetchedColumnChunk} with two PLAIN INT64 pages concatenated in one chunk buffer (required column,
     * no def/rep levels). The value bytes are the little-endian INT64 layout the live-page slice copies straight from.
     */
    private static FetchedColumnChunk twoPageInt64Chunk(long[] page1, long[] page2) throws IOException {
        byte[] payload1 = encodeLongsLittleEndian(page1);
        byte[] payload2 = encodeLongsLittleEndian(page2);
        byte[] page1Bytes = encodeV1Page(page1.length, payload1, org.apache.parquet.format.Encoding.PLAIN);
        byte[] page2Bytes = encodeV1Page(page2.length, payload2, org.apache.parquet.format.Encoding.PLAIN);
        byte[] chunkBuffer = concat(page1Bytes, page2Bytes);
        long totalValues = (long) page1.length + page2.length;
        return heapChunk(PATH, chunkBuffer, totalValues, /*maxRep*/ 0, /*maxDef*/ 0);
    }

    /**
     * Builds a {@link FetchedColumnChunk} with two PLAIN FLOAT pages concatenated in one chunk buffer (required column,
     * no def/rep levels). The value bytes are the little-endian FLOAT layout the live-page slice copies straight from.
     */
    private static FetchedColumnChunk twoPageFloatChunk(float[] page1, float[] page2) throws IOException {
        byte[] payload1 = encodeFloatsLittleEndian(page1);
        byte[] payload2 = encodeFloatsLittleEndian(page2);
        byte[] page1Bytes = encodeV1Page(page1.length, payload1, org.apache.parquet.format.Encoding.PLAIN);
        byte[] page2Bytes = encodeV1Page(page2.length, payload2, org.apache.parquet.format.Encoding.PLAIN);
        byte[] chunkBuffer = concat(page1Bytes, page2Bytes);
        long totalValues = (long) page1.length + page2.length;
        return heapChunk(PATH, chunkBuffer, totalValues, /*maxRep*/ 0, /*maxDef*/ 0);
    }

    /**
     * Builds a {@link FetchedColumnChunk} with a single nullable INT32 page. The V1 payload has a def-level section
     * (length-prefixed) followed by the value bytes for non-null rows only.
     */
    private static FetchedColumnChunk singlePageNullableChunk(int[] nonNullValues, byte[] defBytes, int numValues)
            throws IOException {
        byte[] valueBytes = encodeInt32sLittleEndian(nonNullValues);
        // V1 nullable payload: [4-byte LE def-level length][def-level bytes][value bytes]
        byte[] payload = buildV1PayloadWithDefLevels(defBytes, valueBytes);
        byte[] chunkBuffer = encodeV1Page(numValues, payload, org.apache.parquet.format.Encoding.PLAIN);
        return heapChunk(PATH, chunkBuffer, numValues, /*maxRep*/ 0, /*maxDef*/ 1);
    }

    /**
     * Builds a single nullable INT32 page chunk at an arbitrary (possibly nested) path. A nested path gives the leaf
     * {@code numParts > 1}, which keeps the origin-validity fast path off even at {@code maxDef == 1}.
     */
    private static FetchedColumnChunk nestedNullableChunk(
            ColumnPath path, int[] nonNullValues, byte[] defBytes, int numValues) throws IOException {
        byte[] valueBytes = encodeInt32sLittleEndian(nonNullValues);
        byte[] payload = buildV1PayloadWithDefLevels(defBytes, valueBytes);
        byte[] chunkBuffer = encodeV1Page(numValues, payload, org.apache.parquet.format.Encoding.PLAIN);
        return heapChunk(path, chunkBuffer, numValues, /*maxRep*/ 0, /*maxDef*/ 1);
    }

    /** Encodes one nullable INT32 V1 page: def-level section (length-prefixed) then the non-null value bytes. */
    private static byte[] nullableInt32Page(int[] defLevels, int[] nonNullValues) throws IOException {
        byte[] defBytes = rleEncodeBits(defLevels, /*maxLevel*/ 1);
        byte[] valueBytes = encodeInt32sLittleEndian(nonNullValues);
        byte[] payload = buildV1PayloadWithDefLevels(defBytes, valueBytes);
        return encodeV1Page(defLevels.length, payload, org.apache.parquet.format.Encoding.PLAIN);
    }

    /** Encodes one nullable DOUBLE V1 page: def-level section (length-prefixed) then the non-null value bytes. */
    private static byte[] nullableDoublePage(int[] defLevels, double[] nonNullValues) throws IOException {
        byte[] defBytes = rleEncodeBits(defLevels, /*maxLevel*/ 1);
        byte[] valueBytes = encodeDoublesLittleEndian(nonNullValues);
        byte[] payload = buildV1PayloadWithDefLevels(defBytes, valueBytes);
        return encodeV1Page(defLevels.length, payload, org.apache.parquet.format.Encoding.PLAIN);
    }

    /** Concatenates two already-encoded V1 pages into one INT32 chunk buffer. */
    private static FetchedColumnChunk twoPageChunk(byte[] page1, byte[] page2, long totalValues, int maxDef) {
        byte[] chunkBuffer = concat(page1, page2);
        return heapChunk(PATH, chunkBuffer, totalValues, /*maxRep*/ 0, maxDef);
    }

    /**
     * Serializes a Parquet V1 page header followed by the payload into a byte array. The header uses
     * {@link Util#writePageHeader} (test-scope parquet-format-structures) so the bytes are valid Thrift compact-encoded
     * PageHeader structs that the real {@link PageCursor} can parse.
     */
    private static byte[] encodeV1Page(int numValues, byte[] payload, org.apache.parquet.format.Encoding encoding)
            throws IOException {
        PageHeader header = new PageHeader(PageType.DATA_PAGE, payload.length, payload.length);
        DataPageHeader dataHeader = new DataPageHeader(
                numValues, encoding, org.apache.parquet.format.Encoding.RLE, org.apache.parquet.format.Encoding.RLE);
        header.setData_page_header(dataHeader);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Util.writePageHeader(header, out);
        out.write(payload);
        return out.toByteArray();
    }

    /** A PLAIN V2 page of a required flat INT32 column, with no level sections, stating {@code numRows} rows. */
    private static byte[] encodeV2Page(int numRows, int[] values) throws IOException {
        byte[] payload = encodeInt32sLittleEndian(values);
        PageHeader header = new PageHeader(PageType.DATA_PAGE_V2, payload.length, payload.length);
        DataPageHeaderV2 dataHeader =
                new DataPageHeaderV2(values.length, 0, numRows, org.apache.parquet.format.Encoding.PLAIN, 0, 0);
        header.setData_page_header_v2(dataHeader);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Util.writePageHeader(header, out);
        out.write(payload);
        return out.toByteArray();
    }

    /**
     * Builds the V1 decompressed payload for a nullable column: a 4-byte LE def-level length followed by the def-level
     * bytes, then the value bytes for non-null rows.
     */
    private static byte[] buildV1PayloadWithDefLevels(byte[] defBytes, byte[] valueBytes) {
        ByteBuffer buf =
                ByteBuffer.allocate(4 + defBytes.length + valueBytes.length).order(LITTLE_ENDIAN);
        buf.putInt(defBytes.length);
        buf.put(defBytes);
        buf.put(valueBytes);
        return buf.array();
    }

    /**
     * RLE-encodes a sequence of level values using the Parquet RLE-bit-packed hybrid scheme. For small runs of a single
     * value (all same or alternating), we use the RLE form: varint(runLen << 1) followed by the value in
     * ceil(bitWidth/8) bytes. This helper emits one RLE run per unique value run, which is the simplest encoding to
     * produce by hand for test fixtures.
     *
     * <p>For the nullable test, the levels are 1,0,1,0,1 which requires bit-width 1. We emit each value individually as
     * a single-element RLE run (runLen=1, value=0 or 1) to keep the encoding trivial.
     */
    private static byte[] rleEncodeBits(int[] levels, int maxLevel) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int bitWidth = computeBitWidth(maxLevel);
        int bytesPerValue = (bitWidth + 7) / 8;
        for (int level : levels) {
            // RLE run of length 1: header varint = (1 << 1) | 0 = 2
            writeVarint(out, 2L);
            // Write value in bytesPerValue little-endian bytes
            for (int b = 0; b < bytesPerValue; b++) {
                out.write((level >>> (b * 8)) & 0xff);
            }
        }
        return out.toByteArray();
    }

    private static int computeBitWidth(int maxLevel) {
        if (maxLevel == 0) {
            return 0;
        }
        return 32 - Integer.numberOfLeadingZeros(maxLevel);
    }

    private static void writeVarint(ByteArrayOutputStream out, long value) {
        while (true) {
            if ((value & ~0x7fL) == 0) {
                out.write((int) value);
                return;
            }
            out.write(((int) value & 0x7f) | 0x80);
            value >>>= 7;
        }
    }

    /**
     * Wraps a byte array in a read-only heap {@link MemorySegment} and builds a {@link FetchedColumnChunk} around it.
     * The values are typed by the schema leaf used to open the reader, not by the chunk, which is why one builder
     * serves every physical type.
     */
    private static FetchedColumnChunk heapChunk(ColumnPath path, byte[] data, long numValues, int maxRep, int maxDef) {
        MemorySegment segment = MemorySegment.ofArray(data).asReadOnly();

        return new FetchedColumnChunk(
                path,
                CompressionCodec.UNCOMPRESSED,
                numValues,
                maxRep,
                maxDef,
                List.of(new DataPageRun(segment, 0)),
                Optional.empty());
    }

    /**
     * Builds a {@link FetchedColumnChunk} for a dictionary-encoded INT32 column.
     *
     * <p>The dictionary page holds {@code dictValues} in PLAIN encoding. The data page payload is a RLE-bit-packed
     * index stream: one bit-width byte followed by the RLE-bit-packed-hybrid body (one group of 8 values, padded with
     * zeros). The bit-width is chosen as the minimum needed to represent the largest index value.
     *
     * @param dictValues the unique values in the dictionary
     * @param indices the per-row index references into the dictionary
     */
    private static FetchedColumnChunk dictionaryEncodedInt32Chunk(int[] dictValues, int[] indices) throws IOException {
        DictionaryPage dictionaryPage =
                dictionaryPageOf(PrimitiveKind.INT32, encodeInt32sLittleEndian(dictValues), dictValues.length);

        byte[] pagePayload = encodeRleDictionaryIndexPage(indices, dictValues.length);
        byte[] chunkBuffer =
                encodeV1Page(indices.length, pagePayload, org.apache.parquet.format.Encoding.RLE_DICTIONARY);

        MemorySegment segment = MemorySegment.ofArray(chunkBuffer).asReadOnly();

        return new FetchedColumnChunk(
                PATH,
                CompressionCodec.UNCOMPRESSED,
                (long) indices.length,
                /*maxRep*/ 0,
                /*maxDef*/ 0,
                List.of(new DataPageRun(segment, 0)),
                Optional.of(dictionaryPage));
    }

    /**
     * Encodes {@code indices} as a RLE-bit-packed-hybrid index page payload for use in a RLE_DICTIONARY data page.
     *
     * <p>Layout: one byte for the bit-width, followed by one bit-packed run header, followed by the packed index bytes.
     * The run is padded to a multiple of 8 values (the bit-packed group size).
     *
     * @param indices the index values to encode
     * @param dictionarySize the number of dictionary entries (used to compute minimum bit-width)
     */
    private static byte[] encodeRleDictionaryIndexPage(int[] indices, int dictionarySize) {
        int bitWidth = computeBitWidth(dictionarySize - 1);
        if (bitWidth == 0) {
            bitWidth = 1;
        }
        // Round up indices.length to the next multiple of 8 for bit-packed groups.
        int paddedCount = ((indices.length + 7) / 8) * 8;
        int byteCount = (paddedCount * bitWidth + 7) / 8;
        // One bit-packed group header: (numGroups << 1) | 1; numGroups = paddedCount / 8.
        int numGroups = paddedCount / 8;
        int rleHeader = (numGroups << 1) | 1;

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        // Byte 0: bit width
        out.write(bitWidth);
        // Varint-encoded bit-packed run header
        writeVarint(out, rleHeader);
        // Pack index values LSB-first into bytes
        byte[] packed = new byte[byteCount];
        for (int i = 0; i < indices.length; i++) {
            int bitOffset = i * bitWidth;
            int byteOffset = bitOffset / 8;
            int shift = bitOffset % 8;
            packed[byteOffset] |= (byte) ((indices[i] & ((1 << bitWidth) - 1)) << shift);
            // Handle values spanning byte boundaries
            if (shift + bitWidth > 8) {
                packed[byteOffset + 1] |= (byte) ((indices[i] & ((1 << bitWidth) - 1)) >>> (8 - shift));
            }
        }
        out.write(packed, 0, byteCount);
        return out.toByteArray();
    }

    private static SchemaNode.Primitive requiredInt32Leaf() {
        return new SchemaNode.Primitive(
                "val", Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive requiredDoubleLeaf() {
        return new SchemaNode.Primitive(
                "val", Repetition.REQUIRED, PrimitiveKind.DOUBLE, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive requiredLongLeaf() {
        return new SchemaNode.Primitive(
                "val", Repetition.REQUIRED, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive requiredFloatLeaf() {
        return new SchemaNode.Primitive(
                "val", Repetition.REQUIRED, PrimitiveKind.FLOAT, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive optionalInt32Leaf() {
        return new SchemaNode.Primitive(
                "val", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive optionalDoubleLeaf() {
        return new SchemaNode.Primitive(
                "val", Repetition.OPTIONAL, PrimitiveKind.DOUBLE, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive optionalByteArrayLeaf() {
        return new SchemaNode.Primitive(
                "val", Repetition.OPTIONAL, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Builds a two-page nullable BYTE_ARRAY chunk. Each page row that is non-null becomes a PLAIN byte-array value
     * (4-byte LE length prefix followed by the bytes); a {@code null} row contributes a def level of 0 and no value.
     */
    private static FetchedColumnChunk twoPageNullableByteArrayChunk(byte[][] page1Rows, byte[][] page2Rows)
            throws IOException {
        byte[] page1 = encodeNullableByteArrayPage(page1Rows);
        byte[] page2 = encodeNullableByteArrayPage(page2Rows);
        byte[] chunkBuffer = concat(page1, page2);
        long totalValues = (long) page1Rows.length + page2Rows.length;
        return heapChunk(PATH, chunkBuffer, totalValues, /*maxRep*/ 0, /*maxDef*/ 1);
    }

    private static byte[] encodeNullableByteArrayPage(byte[][] rows) throws IOException {
        int[] defLevels = new int[rows.length];
        for (int row = 0; row < rows.length; row++) {
            defLevels[row] = (rows[row] == null) ? 0 : 1;
        }
        byte[] defLevelBytes = rleEncodeBits(defLevels, /*maxLevel*/ 1);
        byte[] valueBytes = encodePlainByteArrays(rows);
        byte[] payload = buildV1PayloadWithDefLevels(defLevelBytes, valueBytes);
        return encodeV1Page(rows.length, payload, org.apache.parquet.format.Encoding.PLAIN);
    }

    private static byte[] encodePlainByteArrays(byte[][] rows) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] row : rows) {
            if (row == null) {
                continue;
            }
            ByteBuffer lengthPrefix = ByteBuffer.allocate(4).order(LITTLE_ENDIAN);
            lengthPrefix.putInt(row.length);
            out.writeBytes(lengthPrefix.array());
            out.writeBytes(row);
        }
        return out.toByteArray();
    }

    private static byte[] encodeInt32sLittleEndian(int[] values) {
        ByteBuffer buf = ByteBuffer.allocate(values.length * 4).order(LITTLE_ENDIAN);
        for (int v : values) {
            buf.putInt(v);
        }
        return buf.array();
    }

    private static byte[] encodeDoublesLittleEndian(double[] values) {
        ByteBuffer buf = ByteBuffer.allocate(values.length * Double.BYTES).order(LITTLE_ENDIAN);
        for (double v : values) {
            buf.putDouble(v);
        }
        return buf.array();
    }

    private static byte[] encodeLongsLittleEndian(long[] values) {
        ByteBuffer buf = ByteBuffer.allocate(values.length * Long.BYTES).order(LITTLE_ENDIAN);
        for (long v : values) {
            buf.putLong(v);
        }
        return buf.array();
    }

    private static byte[] encodeFloatsLittleEndian(float[] values) {
        ByteBuffer buf = ByteBuffer.allocate(values.length * Float.BYTES).order(LITTLE_ENDIAN);
        for (float v : values) {
            buf.putFloat(v);
        }
        return buf.array();
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] out = new byte[a.length + b.length];
        System.arraycopy(a, 0, out, 0, a.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }
}
