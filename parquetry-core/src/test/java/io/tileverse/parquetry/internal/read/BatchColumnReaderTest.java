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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.batch.BinaryVector;
import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.IntVector;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.CompressionCodec;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.internal.read.page.Dictionary;
import io.tileverse.parquetry.internal.read.page.PageCursor;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

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

    /**
     * Pins the contract of {@link BatchColumnReader#sliceBitSet}: extract bits {@code [start, start + n)} from the
     * source and shift them down so source bit {@code start} maps to slice bit 0, with no bit set at or beyond index
     * {@code n}.
     */
    @Nested
    class SliceBitSet {

        @Test
        void copiesAllBitsWhenFullyValid() {
            BitSet source = new BitSet(100);
            source.set(0, 100);

            BitSet slice = BatchColumnReader.sliceBitSet(source, 0, 100);

            assertThat(slice.cardinality()).isEqualTo(100);
            assertThat(slice.nextClearBit(0)).isEqualTo(100);
        }

        @Test
        void shiftsByStartOffset() {
            BitSet source = new BitSet(100);
            source.set(10, 100);

            BitSet slice = BatchColumnReader.sliceBitSet(source, 8, 50);

            assertThat(slice.get(0)).isFalse();
            assertThat(slice.get(1)).isFalse();
            assertThat(slice.get(2)).isTrue();
            assertThat(slice.cardinality()).isEqualTo(48);
        }

        @Test
        void excludesBitsBeyondTheWindow() {
            BitSet source = new BitSet(200);
            source.set(0, 200);

            BitSet slice = BatchColumnReader.sliceBitSet(source, 50, 64);

            assertThat(slice.cardinality()).isEqualTo(64);
            assertThat(slice.length()).isEqualTo(64);
        }

        @Test
        void preservesSparsePattern() {
            BitSet source = new BitSet(128);
            source.set(65);
            source.set(70);
            source.set(127);

            BitSet slice = BatchColumnReader.sliceBitSet(source, 64, 64);

            assertThat(slice.get(1)).isTrue();
            assertThat(slice.get(6)).isTrue();
            assertThat(slice.get(63)).isTrue();
            assertThat(slice.cardinality()).isEqualTo(3);
        }

        @Test
        void emptyWhenNoBitsInWindow() {
            BitSet source = new BitSet(100);
            source.set(0, 10);

            BitSet slice = BatchColumnReader.sliceBitSet(source, 20, 30);

            assertThat(slice.isEmpty()).isTrue();
        }
    }

    // --- test 1: single page, no nulls, INT32 ---

    @Test
    void readsOnePageIntoIntVector() throws IOException {
        int[] values = {10, 20, 30, 40};
        FetchedColumnChunk chunk = singlePageInt32Chunk(values, /*maxDef*/ 0);
        SchemaNode.Primitive leaf = requiredInt32Leaf();

        BatchColumnReader reader = new BatchColumnReader(chunk, leaf);

        assertThat(reader.hasMore()).isTrue();

        ColumnVector vec = reader.readBatch(10);
        assertThat(vec).isInstanceOf(IntVector.class);
        assertThat(vec.size()).isEqualTo(4);

        BitSet validity = vec.validity();
        assertThat(validity.cardinality()).isEqualTo(4);

        IntVector intVec = (IntVector) vec;
        assertThat(intVec.asArray()).containsExactly(10, 20, 30, 40);

        assertThat(reader.hasMore()).isFalse();
    }

    // --- test 2: two-page chunk, batch stops at page boundary ---

    @Test
    void readBatchStopsAtPageBoundary() throws IOException {
        int[] page1 = {1, 2, 3, 4};
        int[] page2 = {5, 6, 7, 8, 9, 10};
        FetchedColumnChunk chunk = twoPageInt32Chunk(page1, page2);
        SchemaNode.Primitive leaf = requiredInt32Leaf();

        BatchColumnReader reader = new BatchColumnReader(chunk, leaf);

        // Ask for 7 rows - should only get 4 (page 1 has only 4)
        ColumnVector vec1 = reader.readBatch(7);
        assertThat(vec1.size()).isEqualTo(4);
        assertThat(((IntVector) vec1).asArray()).containsExactly(1, 2, 3, 4);

        // Still has more (page 2 is pending)
        assertThat(reader.hasMore()).isTrue();

        // Ask for 7 again - gets all 6 from page 2
        ColumnVector vec2 = reader.readBatch(7);
        assertThat(vec2.size()).isEqualTo(6);
        assertThat(((IntVector) vec2).asArray()).containsExactly(5, 6, 7, 8, 9, 10);

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

        BatchColumnReader reader = new BatchColumnReader(chunk, leaf);
        ColumnVector vec = reader.readBatch(10);

        assertThat(vec.size()).isEqualTo(5);
        BitSet validity = vec.validity();
        // Rows 0, 2, 4 are non-null.
        assertThat(validity.get(0)).isTrue();
        assertThat(validity.get(1)).isFalse();
        assertThat(validity.get(2)).isTrue();
        assertThat(validity.get(3)).isFalse();
        assertThat(validity.get(4)).isTrue();
    }

    // --- test 4: hasMore false after all pages consumed ---

    @Test
    void hasMoreFalseAfterAllPagesConsumed() throws IOException {
        int[] page1 = {1, 2};
        int[] page2 = {3, 4};
        FetchedColumnChunk chunk = twoPageInt32Chunk(page1, page2);
        SchemaNode.Primitive leaf = requiredInt32Leaf();

        BatchColumnReader reader = new BatchColumnReader(chunk, leaf);

        assertThat(reader.hasMore()).isTrue();
        reader.readBatch(10); // drains page 1
        assertThat(reader.hasMore()).isTrue();
        reader.readBatch(10); // drains page 2
        assertThat(reader.hasMore()).isFalse();
    }

    // --- test 5: dictionary-encoded column resolves indices through the dictionary ---

    @Test
    void readsDictionaryEncodedColumn() throws IOException {
        // Three unique values: 100, 200, 300. Index page references them as 0,1,2,1,0.
        // Expected materialized output: 100, 200, 300, 200, 100.
        FetchedColumnChunk chunk = dictionaryEncodedInt32Chunk(new int[] {100, 200, 300}, new int[] {0, 1, 2, 1, 0});

        BatchColumnReader reader = new BatchColumnReader(chunk, requiredInt32Leaf());

        ColumnVector vec = reader.readBatch(10);
        assertThat(vec).isInstanceOf(IntVector.class);
        assertThat(vec.size()).isEqualTo(5);
        assertThat(((IntVector) vec).asArray()).containsExactly(100, 200, 300, 200, 100);

        assertThat(reader.hasMore()).isFalse();
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

            BatchColumnReader reader = new BatchColumnReader(chunk, optionalByteArrayLeaf());

            // First batch: 3 of page 1's rows.
            ColumnVector batch1 = reader.readBatch(3);
            assertByteArraySlice(batch1, new byte[][] {bytes("aa"), null, bytes("bbbb")});

            // Second batch asks for 5 but only 2 remain in page 1.
            ColumnVector batch2 = reader.readBatch(5);
            assertByteArraySlice(batch2, new byte[][] {null, bytes("c")});

            assertThat(reader.hasMore()).isTrue();

            // Third batch drains page 2.
            ColumnVector batch3 = reader.readBatch(10);
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

            BatchColumnReader reader = new BatchColumnReader(chunk, optionalByteArrayLeaf());

            // Page 1 is all-present: first batch takes 2 of its 3 rows.
            ColumnVector batch1 = reader.readBatch(2);
            assertByteArraySlice(batch1, new byte[][] {bytes("aa"), bytes("bbbb")});

            // Second batch asks for 2 but only page 1's last row remains; readBatch never crosses a page.
            ColumnVector batch2 = reader.readBatch(2);
            assertByteArraySlice(batch2, new byte[][] {bytes("c")});

            assertThat(reader.hasMore()).isTrue();

            // Page 2 is all-null: an empty backing with two null rows.
            ColumnVector batch3 = reader.readBatch(2);
            assertByteArraySlice(batch3, new byte[][] {null, null});

            assertThat(reader.hasMore()).isFalse();
        }

        private void assertByteArraySlice(ColumnVector vec, byte[][] expected) {
            assertThat(vec).isInstanceOf(BinaryVector.class);
            assertThat(vec.size()).isEqualTo(expected.length);
            BinaryVector binary = (BinaryVector) vec;
            for (int row = 0; row < expected.length; row++) {
                if (expected[row] == null) {
                    assertThat(binary.validity().get(row))
                            .as("row %d is null", row)
                            .isFalse();
                } else {
                    assertThat(binary.validity().get(row))
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

            BatchColumnReader reader = new BatchColumnReader(chunk, optionalByteArrayLeaf());

            // First batch: rows 0..2 -> "alpha", null, "beta".
            ColumnVector batch1 = reader.readBatch(3);
            assertByteArraySlice(batch1, new byte[][] {bytes("alpha"), null, bytes("beta")});

            // Second batch: rows 3..5 -> "alpha", null, "beta".
            ColumnVector batch2 = reader.readBatch(3);
            assertByteArraySlice(batch2, new byte[][] {bytes("alpha"), null, bytes("beta")});

            assertThat(reader.hasMore()).isFalse();

            // The two "alpha" rows (batch1 row 0, batch2 row 0) reference the same dictionary entry instance.
            MemorySegment firstAlpha = ((BinaryVector) batch1).get(0);
            MemorySegment secondAlpha = ((BinaryVector) batch2).get(0);
            assertThat(firstAlpha).isSameAs(secondAlpha);
        }

        private void assertByteArraySlice(ColumnVector vec, byte[][] expected) {
            assertThat(vec).isInstanceOf(BinaryVector.class);
            assertThat(vec.size()).isEqualTo(expected.length);
            BinaryVector binary = (BinaryVector) vec;
            for (int row = 0; row < expected.length; row++) {
                if (expected[row] == null) {
                    assertThat(binary.validity().get(row))
                            .as("row %d is null", row)
                            .isFalse();
                } else {
                    assertThat(binary.validity().get(row))
                            .as("row %d is present", row)
                            .isTrue();
                    assertThat(binary.get(row).toArray(JAVA_BYTE))
                            .as("row %d bytes", row)
                            .containsExactly(expected[row]);
                }
            }
        }
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
        Dictionary.BinaryDict dictionary = new Dictionary.BinaryDict(toSegments(dictValues));

        byte[] defLevelBytes = rleEncodeBits(defLevels, /*maxLevel*/ 1);
        byte[] indexPage = encodeRleDictionaryIndexPage(indices, dictValues.length);
        byte[] payload = buildV1PayloadWithDefLevels(defLevelBytes, indexPage);
        byte[] chunkBuffer = encodeV1Page(defLevels.length, payload, org.apache.parquet.format.Encoding.RLE_DICTIONARY);

        MemorySegment segment = MemorySegment.ofArray(chunkBuffer).asReadOnly();

        ColumnMetaData meta = ColumnMetaData.builder()
                .type(PhysicalType.BYTE_ARRAY)
                .encodings(List.of(Encoding.RLE_DICTIONARY))
                .pathInSchema(pathSegments(PATH))
                .codec(CompressionCodec.UNCOMPRESSED)
                .numValues((long) defLevels.length)
                .totalUncompressedSize((long) chunkBuffer.length)
                .totalCompressedSize((long) chunkBuffer.length)
                .dataPageOffset(0L)
                .build();

        return new FetchedColumnChunk(PATH, meta, /*maxRep*/ 0, /*maxDef*/ 1, segment, Optional.of(dictionary));
    }

    private static List<MemorySegment> toSegments(byte[][] values) {
        return java.util.Arrays.stream(values)
                .map(value -> MemorySegment.ofArray(value).asReadOnly())
                .toList();
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

    private static List<String> pathSegments(ColumnPath path) {
        String[] segments = new String[path.numParts()];
        for (int i = 0; i < segments.length; i++) {
            segments[i] = path.part(i);
        }
        return List.of(segments);
    }

    /**
     * Wraps a byte array in a read-only heap {@link MemorySegment} and builds a {@link FetchedColumnChunk} around it.
     */
    private static FetchedColumnChunk heapChunk(ColumnPath path, byte[] data, long numValues, int maxRep, int maxDef) {
        MemorySegment segment = MemorySegment.ofArray(data).asReadOnly();

        ColumnMetaData meta = ColumnMetaData.builder()
                .type(PhysicalType.INT32)
                .encodings(java.util.List.of(Encoding.PLAIN))
                .pathInSchema(pathSegments(path))
                .codec(CompressionCodec.UNCOMPRESSED)
                .numValues(numValues)
                .totalUncompressedSize((long) data.length)
                .totalCompressedSize((long) data.length)
                .dataPageOffset(0L)
                .build();

        return new FetchedColumnChunk(path, meta, maxRep, maxDef, segment, Optional.empty());
    }

    /**
     * Builds a {@link FetchedColumnChunk} for a dictionary-encoded INT32 column.
     *
     * <p>The dictionary is built directly from {@code dictValues}. The data page payload is a RLE-bit-packed index
     * stream: one bit-width byte followed by the RLE-bit-packed-hybrid body (one group of 8 values, padded with zeros).
     * The bit-width is chosen as the minimum needed to represent the largest index value.
     *
     * @param dictValues the unique values in the dictionary
     * @param indices the per-row index references into the dictionary
     */
    private static FetchedColumnChunk dictionaryEncodedInt32Chunk(int[] dictValues, int[] indices) throws IOException {
        Dictionary.IntDict dictionary =
                new Dictionary.IntDict(IntBuffer.wrap(dictValues).asReadOnlyBuffer());

        byte[] pagePayload = encodeRleDictionaryIndexPage(indices, dictValues.length);
        byte[] chunkBuffer =
                encodeV1Page(indices.length, pagePayload, org.apache.parquet.format.Encoding.RLE_DICTIONARY);

        MemorySegment segment = MemorySegment.ofArray(chunkBuffer).asReadOnly();

        ColumnMetaData meta = ColumnMetaData.builder()
                .type(PhysicalType.INT32)
                .encodings(List.of(Encoding.RLE_DICTIONARY))
                .pathInSchema(pathSegments(PATH))
                .codec(CompressionCodec.UNCOMPRESSED)
                .numValues((long) indices.length)
                .totalUncompressedSize((long) chunkBuffer.length)
                .totalCompressedSize((long) chunkBuffer.length)
                .dataPageOffset(0L)
                .build();

        return new FetchedColumnChunk(PATH, meta, /*maxRep*/ 0, /*maxDef*/ 0, segment, Optional.of(dictionary));
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

    private static SchemaNode.Primitive optionalInt32Leaf() {
        return new SchemaNode.Primitive(
                "val", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
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
        return byteArrayHeapChunk(chunkBuffer, totalValues, /*maxDef*/ 1);
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

    private static FetchedColumnChunk byteArrayHeapChunk(byte[] data, long numValues, int maxDef) {
        MemorySegment segment = MemorySegment.ofArray(data).asReadOnly();

        ColumnMetaData meta = ColumnMetaData.builder()
                .type(PhysicalType.BYTE_ARRAY)
                .encodings(List.of(Encoding.PLAIN))
                .pathInSchema(pathSegments(PATH))
                .codec(CompressionCodec.UNCOMPRESSED)
                .numValues(numValues)
                .totalUncompressedSize((long) data.length)
                .totalCompressedSize((long) data.length)
                .dataPageOffset(0L)
                .build();

        return new FetchedColumnChunk(PATH, meta, /*maxRep*/ 0, maxDef, segment, Optional.empty());
    }

    private static byte[] encodeInt32sLittleEndian(int[] values) {
        ByteBuffer buf = ByteBuffer.allocate(values.length * 4).order(LITTLE_ENDIAN);
        for (int v : values) {
            buf.putInt(v);
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
