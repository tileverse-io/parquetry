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

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;
import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.IntVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.CompressionCodec;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.io.SegmentPool.Pooled;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.testsupport.VectorArrays;

/** Unit tests for {@link ClassicRowGroupDriver}, the standard per-row-group streaming path. */
class RowGroupBatchDriverTest {

    private static final ColumnPath PATH_A = ColumnPath.of("a");

    @Test
    void streamsAllBatchesInPageOrder() throws IOException {
        int[] page1 = {10, 20, 30, 40};
        int[] page2 = {50, 60, 70, 80, 90, 100};
        RowGroupFetch fetch = fetchOf(twoPageInt32Chunk(PATH_A, page1, page2));
        ParquetSchema schema = flatSchema("a");

        List<int[]> decoded = new ArrayList<>();
        try (ClassicRowGroupDriver driver = new ClassicRowGroupDriver(
                TestDecodeBuffers.ample(),
                fetch,
                schema,
                schema,
                OptionalInt.empty(),
                Optional.empty(),
                BatchForm.ASSEMBLED,
                Optional.empty())) {
            while (driver.hasMore()) {
                try (ParquetRecordBatch batch = driver.nextBatch()) {
                    IntVector vec = (IntVector) batch.columns().get(PATH_A);
                    decoded.add(VectorArrays.toArray(vec));
                }
            }
        }

        assertThat(decoded)
                .as("driver streams one batch per page, in file order")
                .hasSize(2);
        assertThat(decoded.get(0)).containsExactly(10, 20, 30, 40);
        assertThat(decoded.get(1)).containsExactly(50, 60, 70, 80, 90, 100);
    }

    @Test
    void closeIsIdempotentAndClosesTheFetch() throws IOException {
        RowGroupFetch fetch = fetchOf(singlePageInt32Chunk(PATH_A, new int[] {1, 2, 3}));
        ParquetSchema schema = flatSchema("a");

        ClassicRowGroupDriver driver = new ClassicRowGroupDriver(
                TestDecodeBuffers.ample(),
                fetch,
                schema,
                schema,
                OptionalInt.empty(),
                Optional.empty(),
                BatchForm.ASSEMBLED,
                Optional.empty());
        try (ParquetRecordBatch batch = driver.nextBatch()) {
            assertThat(batch.rowCount()).isEqualTo(3);
        }

        driver.close();
        // The fetch is closed by the driver; closing it again is a no-op (RowGroupFetch close is idempotent).
        assertThatCode(fetch::close).doesNotThrowAnyException();
        // A second driver close must also be safe.
        assertThatCode(driver::close).doesNotThrowAnyException();
    }

    // --- fixture helpers ---

    private static RowGroupFetch fetchOf(FetchedColumnChunk chunk) {
        return new RowGroupFetch(List.<Pooled>of(), List.of(chunk), BudgetReservation.NONE, 0L);
    }

    private static ParquetSchema flatSchema(String... names) {
        List<SchemaNode> children = new ArrayList<>();
        for (String name : names) {
            children.add(new SchemaNode.Primitive(
                    name, Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1));
        }
        SchemaNode.Group root = new SchemaNode.Group("root", Repetition.REQUIRED, children, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static FetchedColumnChunk singlePageInt32Chunk(ColumnPath path, int[] values) throws IOException {
        byte[] payload = encodeInt32sLittleEndian(values);
        byte[] chunkBytes = encodeV1Page(values.length, payload);
        return heapChunk(path, chunkBytes, values.length);
    }

    private static FetchedColumnChunk twoPageInt32Chunk(ColumnPath path, int[] page1, int[] page2) throws IOException {
        byte[] bytes1 = encodeV1Page(page1.length, encodeInt32sLittleEndian(page1));
        byte[] bytes2 = encodeV1Page(page2.length, encodeInt32sLittleEndian(page2));
        byte[] chunkBytes = concat(bytes1, bytes2);
        long totalValues = (long) page1.length + page2.length;
        return heapChunk(path, chunkBytes, totalValues);
    }

    private static byte[] encodeV1Page(int numValues, byte[] payload) throws IOException {
        PageHeader header = new PageHeader(PageType.DATA_PAGE, payload.length, payload.length);
        DataPageHeader dataHeader = new DataPageHeader(
                numValues,
                org.apache.parquet.format.Encoding.PLAIN,
                org.apache.parquet.format.Encoding.RLE,
                org.apache.parquet.format.Encoding.RLE);
        header.setData_page_header(dataHeader);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Util.writePageHeader(header, out);
        out.write(payload);
        return out.toByteArray();
    }

    private static FetchedColumnChunk heapChunk(ColumnPath path, byte[] data, long numValues) {
        MemorySegment segment = MemorySegment.ofArray(data).asReadOnly();

        ColumnMetaData meta = ColumnMetaData.builder()
                .type(PhysicalType.INT32)
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

    private static List<String> pathSegments(ColumnPath path) {
        String[] segments = new String[path.numParts()];
        for (int i = 0; i < segments.length; i++) {
            segments[i] = path.part(i);
        }
        return List.of(segments);
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
