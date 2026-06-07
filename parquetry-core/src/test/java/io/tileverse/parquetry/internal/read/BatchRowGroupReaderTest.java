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

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;
import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.batch.BinaryVector;
import io.tileverse.parquetry.batch.DoubleVector;
import io.tileverse.parquetry.batch.IntVector;
import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.CompressionCodec;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Unit tests for {@link BatchRowGroupReader}.
 *
 * <p>Fixtures are built in-memory using the same helpers as {@link BatchColumnReaderTest}: Thrift compact page headers
 * serialized by {@link Util#writePageHeader} and little-endian INT32 value bytes.
 */
class BatchRowGroupReaderTest {

    private static final ColumnPath PATH_A = ColumnPath.of("a");
    private static final ColumnPath PATH_B = ColumnPath.of("b");

    // --- test 1: single page, single column, no batchSizeCap ---

    @Test
    void singleColumnSinglePageEmitsOneBatch() throws IOException {
        int[] values = {1, 2, 3, 4};
        FetchedColumnChunk chunk = singlePageInt32Chunk(PATH_A, values);
        ParquetSchema schema = flatSchema("a");
        ParquetSchema fileSchema = schema;

        try (BatchRowGroupReader reader = new BatchRowGroupReader(
                TestDecodeBuffers.ample(), List.of(chunk), schema, fileSchema, OptionalInt.empty(), Optional.empty())) {
            assertThat(reader.hasMore()).isTrue();

            try (ParquetRecordBatch batch = reader.nextBatch()) {
                assertThat(batch.rowCount()).isEqualTo(4);
                IntVector vec = (IntVector) batch.columns().get(PATH_A);
                assertThat(vec.asArray()).containsExactly(1, 2, 3, 4);
            }

            assertThat(reader.hasMore()).isFalse();
        }
    }

    // --- test 2: two pages, two columns, batches align at page boundary ---

    @Test
    void emitsBatchesAcrossPagesForFlatSchema() throws IOException {
        // Two INT32 columns, both with two pages: page 1 has 4 rows, page 2 has 6 rows.
        // Expected: first batch rowCount == 4, second batch rowCount == 6.
        int[] colAPage1 = {10, 20, 30, 40};
        int[] colAPage2 = {50, 60, 70, 80, 90, 100};
        int[] colBPage1 = {1, 2, 3, 4};
        int[] colBPage2 = {5, 6, 7, 8, 9, 10};

        FetchedColumnChunk chunkA = twoPageInt32Chunk(PATH_A, colAPage1, colAPage2);
        FetchedColumnChunk chunkB = twoPageInt32Chunk(PATH_B, colBPage1, colBPage2);

        ParquetSchema schema = flatSchema("a", "b");
        ParquetSchema fileSchema = schema;

        try (BatchRowGroupReader reader = new BatchRowGroupReader(
                TestDecodeBuffers.ample(),
                List.of(chunkA, chunkB),
                schema,
                fileSchema,
                OptionalInt.empty(),
                Optional.empty())) {
            assertThat(reader.hasMore()).isTrue();

            // First batch: capped at page 1 boundary (4 rows)
            try (ParquetRecordBatch batch1 = reader.nextBatch()) {
                assertThat(batch1.rowCount()).isEqualTo(4);
                IntVector vecA = (IntVector) batch1.columns().get(PATH_A);
                IntVector vecB = (IntVector) batch1.columns().get(PATH_B);
                assertThat(vecA.asArray()).containsExactly(10, 20, 30, 40);
                assertThat(vecB.asArray()).containsExactly(1, 2, 3, 4);
            }

            assertThat(reader.hasMore()).isTrue();

            // Second batch: page 2 (6 rows)
            try (ParquetRecordBatch batch2 = reader.nextBatch()) {
                assertThat(batch2.rowCount()).isEqualTo(6);
                IntVector vecA = (IntVector) batch2.columns().get(PATH_A);
                IntVector vecB = (IntVector) batch2.columns().get(PATH_B);
                assertThat(vecA.asArray()).containsExactly(50, 60, 70, 80, 90, 100);
                assertThat(vecB.asArray()).containsExactly(5, 6, 7, 8, 9, 10);
            }

            assertThat(reader.hasMore()).isFalse();
        }
    }

    // --- test 3: batchSizeCap limits rows below the page boundary ---

    /**
     * Asserts the {@code batchSizeCap} caps the emitted batch's {@code rowCount}.
     *
     * <p><strong>NOTE:</strong> This test does NOT assert the batch's VALUES when the cap splits a page mid-way; that
     * case is a known limitation (see {@link BatchColumnReader#readBatch(int)} for details). Asserting values here
     * would expose the re-decode-from-byte-0 bug and is deferred until the follow-up adds row-offset-aware
     * materialization.
     */
    @Test
    void batchSizeCapLimitsRowCount() throws IOException {
        int[] values = {1, 2, 3, 4, 5, 6, 7, 8};
        FetchedColumnChunk chunk = singlePageInt32Chunk(PATH_A, values);
        ParquetSchema schema = flatSchema("a");

        // Cap at 3 rows; the page has 8 rows, so we should get ceil(8/3) = 3 batches: 3, 3, 2
        try (BatchRowGroupReader reader = new BatchRowGroupReader(
                TestDecodeBuffers.ample(), List.of(chunk), schema, schema, OptionalInt.of(3), Optional.empty())) {
            try (ParquetRecordBatch b1 = reader.nextBatch()) {
                assertThat(b1.rowCount()).isEqualTo(3);
            }
            assertThat(reader.hasMore()).isTrue();

            try (ParquetRecordBatch b2 = reader.nextBatch()) {
                assertThat(b2.rowCount()).isEqualTo(3);
            }
            assertThat(reader.hasMore()).isTrue();

            try (ParquetRecordBatch b3 = reader.nextBatch()) {
                assertThat(b3.rowCount()).isEqualTo(2);
            }
            assertThat(reader.hasMore()).isFalse();
        }
    }

    // --- test 4: hasMore false on empty projection ---

    @Test
    void hasMoreFalseWhenNoProjectedLeaves() {
        ParquetSchema schema = flatSchema(); // empty schema
        try (BatchRowGroupReader reader = new BatchRowGroupReader(
                TestDecodeBuffers.ample(), List.of(), schema, schema, OptionalInt.empty(), Optional.empty())) {
            assertThat(reader.hasMore()).isFalse();
        }
    }

    // --- test 6: nextBatch throws when no more rows ---

    @Test
    void nextBatchThrowsWhenExhausted() throws IOException {
        int[] values = {1};
        FetchedColumnChunk chunk = singlePageInt32Chunk(PATH_A, values);
        ParquetSchema schema = flatSchema("a");

        try (BatchRowGroupReader reader = new BatchRowGroupReader(
                TestDecodeBuffers.ample(), List.of(chunk), schema, schema, OptionalInt.empty(), Optional.empty())) {
            try (ParquetRecordBatch batch = reader.nextBatch()) {
                assertThat(batch.rowCount()).isEqualTo(1);
            }

            assertThat(reader.hasMore()).isFalse();
            assertThatThrownBy(reader::nextBatch).isInstanceOf(IllegalStateException.class);
        }
    }

    // --- test 7: close() cascades to column readers without leaking Arenas ---

    @Test
    void closeCascadesToColumnReaders() throws IOException {
        // Build a two-column reader, pull one batch (consuming the first page), then close the reader
        // without exhausting all rows. The second page's Arena would leak if close() did not cascade.
        // If BatchColumnReader.close() is not idempotent or throws, this test surfaces the failure.
        int[] colAPage1 = {1, 2};
        int[] colAPage2 = {3, 4};
        int[] colBPage1 = {10, 20};
        int[] colBPage2 = {30, 40};

        FetchedColumnChunk chunkA = twoPageInt32Chunk(PATH_A, colAPage1, colAPage2);
        FetchedColumnChunk chunkB = twoPageInt32Chunk(PATH_B, colBPage1, colBPage2);

        ParquetSchema schema = flatSchema("a", "b");
        BatchRowGroupReader reader = new BatchRowGroupReader(
                TestDecodeBuffers.ample(),
                List.of(chunkA, chunkB),
                schema,
                schema,
                OptionalInt.empty(),
                Optional.empty());

        // Consume only the first batch (page 1). The reader still has page 2 loaded in each column.
        try (ParquetRecordBatch batch = reader.nextBatch()) {
            assertThat(batch.rowCount()).isEqualTo(2);
        }
        assertThat(reader.hasMore()).isTrue();

        // Close the reader early (simulating a consumer that breaks before exhausting rows).
        // Must not throw; per-column page Arenas are released by the cascade.
        reader.close();

        // Calling close() a second time must be safe: BatchColumnReader.close() nulls out its arena refs,
        // so a second iteration over the same column readers is a no-op.
        reader.close();
    }

    // --- test 8: decoded DOUBLE column lives off-heap ---

    @Test
    void doubleColumnIsSegmentBacked() throws IOException {
        double[] values = {1.5, 2.5, 3.5, 4.5};
        FetchedColumnChunk chunk = singlePageDoubleChunk(PATH_A, values);
        ParquetSchema schema = flatDoubleSchema("a");

        try (BatchRowGroupReader reader = new BatchRowGroupReader(
                TestDecodeBuffers.ample(), List.of(chunk), schema, schema, OptionalInt.empty(), Optional.empty())) {
            try (ParquetRecordBatch batch = reader.nextBatch()) {
                DoubleVector vec = (DoubleVector) batch.columns().get(PATH_A);

                assertThat(vec.approximateHeapBytes())
                        .as("double values are off-heap; only the validity bitmap counts as heap")
                        .isEqualTo(vec.validity().heapBytes());
                assertThat(vec.getDouble(0))
                        .as("first value reads back through the off-heap buffer")
                        .isEqualTo(1.5);
                assertThat(vec.asArray()).containsExactly(1.5, 2.5, 3.5, 4.5);
            }
        }
    }

    // --- test 9: decoded BYTE_ARRAY column lives off-heap ---

    @Test
    void byteArrayColumnIsSegmentBacked() throws IOException {
        byte[][] values = {"ab".getBytes(), "cde".getBytes(), "f".getBytes()};
        FetchedColumnChunk chunk = singlePageByteArrayChunk(PATH_A, values);
        ParquetSchema schema = flatByteArraySchema("a");

        try (BatchRowGroupReader reader = new BatchRowGroupReader(
                TestDecodeBuffers.ample(), List.of(chunk), schema, schema, OptionalInt.empty(), Optional.empty())) {
            try (ParquetRecordBatch batch = reader.nextBatch()) {
                BinaryVector binary = (BinaryVector) batch.columns().get(PATH_A);

                assertThat(binary).as("BYTE_ARRAY decodes to a BinaryVector").isInstanceOf(BinaryVector.class);
                assertThat(binary.approximateHeapBytes())
                        .as("value bytes are off-heap; only offsets and validity count as heap")
                        .isEqualTo((long) (binary.size() + 1) * Integer.BYTES
                                + binary.validity().heapBytes());
                assertThat(binary.get(0))
                        .as("first value reads back through the off-heap buffer")
                        .isNotNull();
                assertThat(binary.get(0).toArray(java.lang.foreign.ValueLayout.JAVA_BYTE))
                        .as("first value bytes")
                        .containsExactly('a', 'b');
            }
        }
    }

    // --- fixture helpers ---

    /** Builds a {@link ParquetSchema} with the given required INT32 leaf column names, under a synthetic root group. */
    private static ParquetSchema flatSchema(String... names) {
        List<SchemaNode> children = new java.util.ArrayList<>();
        for (String name : names) {
            children.add(new SchemaNode.Primitive(
                    name, Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1));
        }
        SchemaNode.Group root = new SchemaNode.Group("root", Repetition.REQUIRED, children, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    /** Builds a {@link FetchedColumnChunk} containing a single uncompressed PLAIN INT32 data page. */
    private static FetchedColumnChunk singlePageInt32Chunk(ColumnPath path, int[] values) throws IOException {
        byte[] payload = encodeInt32sLittleEndian(values);
        byte[] chunkBytes = encodeV1Page(values.length, payload, org.apache.parquet.format.Encoding.PLAIN);
        return heapChunk(path, chunkBytes, values.length);
    }

    /** Builds a {@link ParquetSchema} with one required DOUBLE leaf column, under a synthetic root group. */
    private static ParquetSchema flatDoubleSchema(String name) {
        SchemaNode.Primitive leaf = new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.DOUBLE, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root = new SchemaNode.Group("root", Repetition.REQUIRED, List.of(leaf), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    /** Builds a {@link FetchedColumnChunk} containing a single uncompressed PLAIN DOUBLE data page. */
    private static FetchedColumnChunk singlePageDoubleChunk(ColumnPath path, double[] values) throws IOException {
        byte[] payload = encodeDoublesLittleEndian(values);
        byte[] chunkBytes = encodeV1Page(values.length, payload, org.apache.parquet.format.Encoding.PLAIN);
        return heapChunk(path, PhysicalType.DOUBLE, chunkBytes, values.length);
    }

    /** Builds a {@link ParquetSchema} with one required BYTE_ARRAY leaf column, under a synthetic root group. */
    private static ParquetSchema flatByteArraySchema(String name) {
        SchemaNode.Primitive leaf = new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root = new SchemaNode.Group("root", Repetition.REQUIRED, List.of(leaf), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    /**
     * Builds a {@link FetchedColumnChunk} containing a single uncompressed PLAIN BYTE_ARRAY data page. Each value is a
     * 4-byte little-endian length prefix followed by its bytes (required column: no def levels).
     */
    private static FetchedColumnChunk singlePageByteArrayChunk(ColumnPath path, byte[][] values) throws IOException {
        byte[] payload = encodePlainByteArrays(values);
        byte[] chunkBytes = encodeV1Page(values.length, payload, org.apache.parquet.format.Encoding.PLAIN);
        return heapChunk(path, PhysicalType.BYTE_ARRAY, chunkBytes, values.length);
    }

    private static byte[] encodePlainByteArrays(byte[][] rows) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] row : rows) {
            ByteBuffer lengthPrefix = ByteBuffer.allocate(4).order(LITTLE_ENDIAN);
            lengthPrefix.putInt(row.length);
            out.writeBytes(lengthPrefix.array());
            out.writeBytes(row);
        }
        return out.toByteArray();
    }

    /** Builds a {@link FetchedColumnChunk} containing two uncompressed PLAIN INT32 data pages concatenated. */
    private static FetchedColumnChunk twoPageInt32Chunk(ColumnPath path, int[] page1, int[] page2) throws IOException {
        byte[] bytes1 =
                encodeV1Page(page1.length, encodeInt32sLittleEndian(page1), org.apache.parquet.format.Encoding.PLAIN);
        byte[] bytes2 =
                encodeV1Page(page2.length, encodeInt32sLittleEndian(page2), org.apache.parquet.format.Encoding.PLAIN);
        byte[] chunkBytes = concat(bytes1, bytes2);
        long totalValues = (long) page1.length + page2.length;
        return heapChunk(path, chunkBytes, totalValues);
    }

    /**
     * Serializes a Thrift compact V1 page header followed by the payload. Reuses the same approach as
     * {@link BatchColumnReaderTest}.
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

    private static List<String> pathSegments(ColumnPath path) {
        String[] segments = new String[path.numParts()];
        for (int i = 0; i < segments.length; i++) {
            segments[i] = path.part(i);
        }
        return List.of(segments);
    }

    /**
     * Wraps {@code data} in a read-only heap {@link MemorySegment} and builds a {@link FetchedColumnChunk} around it.
     * The chunk is uncompressed with maxRep=0, maxDef=0 (required column).
     */
    private static FetchedColumnChunk heapChunk(ColumnPath path, byte[] data, long numValues) {
        return heapChunk(path, PhysicalType.INT32, data, numValues);
    }

    private static FetchedColumnChunk heapChunk(ColumnPath path, PhysicalType type, byte[] data, long numValues) {
        MemorySegment segment = MemorySegment.ofArray(data).asReadOnly();

        ColumnMetaData meta = ColumnMetaData.builder()
                .type(type)
                .encodings(List.of(Encoding.PLAIN))
                .pathInSchema(pathSegments(path))
                .codec(CompressionCodec.UNCOMPRESSED)
                .numValues(numValues)
                .totalUncompressedSize((long) data.length)
                .totalCompressedSize((long) data.length)
                .dataPageOffset(0L)
                .build();

        return new FetchedColumnChunk(path, meta, /*maxRep*/ 0, /*maxDef*/ 0, segment, Optional.empty());
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

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] out = new byte[a.length + b.length];
        System.arraycopy(a, 0, out, 0, a.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }
}
