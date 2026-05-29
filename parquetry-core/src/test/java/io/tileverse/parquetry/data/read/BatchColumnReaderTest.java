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
package io.tileverse.parquetry.data.read;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.BitSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;
import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.IntVector;
import io.tileverse.parquetry.data.read.page.Dictionary;
import io.tileverse.parquetry.data.read.page.PageCursor;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.CompressionCodec;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.PhysicalType;
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

    // --- fixture helpers ---

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

    /**
     * Wraps a byte array in a read-only heap {@link MemorySegment} and builds a {@link FetchedColumnChunk} around it.
     */
    private static FetchedColumnChunk heapChunk(ColumnPath path, byte[] data, long numValues, int maxRep, int maxDef) {
        MemorySegment segment = MemorySegment.ofArray(data).asReadOnly();

        ColumnMetaData meta = ColumnMetaData.builder()
                .type(PhysicalType.INT32)
                .encodings(java.util.List.of(Encoding.PLAIN))
                .pathInSchema(path.parts())
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
                .pathInSchema(PATH.parts())
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
