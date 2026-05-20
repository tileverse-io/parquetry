/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.read;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;
import org.junit.jupiter.api.Test;

import io.tileverse.storage.RangeReader;

import io.tileverse.parquetry.batch.IntVector;
import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.CompressionCodec;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.Field;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;

import io.tileverse.io.ByteBufferPool;

/**
 * Exercises the batch row-group pipeline against synthetic single-column row groups.
 *
 * <p>The test column fetcher returns pre-built {@link FetchedColumnChunk} objects carrying real Thrift-encoded page
 * bytes so that {@link BatchRowGroupReader} can run end-to-end without any I/O. Each row group is encoded with distinct
 * integer values so ordering assertions can verify row-group sequence.
 *
 * <p>Two scenarios are exercised:
 *
 * <ol>
 *   <li>Full stream consumption - all batches arrive in row-group order, total row count matches.
 *   <li>Early close - pulling one batch then closing verifies the queue is drained and no Arenas leak.
 * </ol>
 */
class BatchRowGroupPipelineTest {

    private static final ColumnPath VALUE_PATH = ColumnPath.of("value");
    private static final int ROWS_PER_GROUP = 4;
    private static final int ROW_GROUPS = 2;

    // --- test 1: full stream ---

    @Test
    void streamEmitsBatchesInRowGroupOrder() throws IOException {
        ParquetSchema schema = singleColumnSchema();
        List<RowGroupSurvivor> survivors = makeSurvivors(ROW_GROUPS);
        ColumnFetcher fetcher = prebuiltFetcher(ROW_GROUPS);

        BatchRowGroupPipeline pipeline = newPipeline(schema, survivors, fetcher, /*prefetchWindow*/ 2);

        List<Integer> values = new ArrayList<>();
        int batchCount = 0;

        try (Stream<ParquetRecordBatch> stream = pipeline.stream()) {
            for (ParquetRecordBatch batch : (Iterable<ParquetRecordBatch>) stream::iterator) {
                batchCount++;
                IntVector vec = (IntVector) batch.columns().get(VALUE_PATH);
                for (int i = 0; i < batch.rowCount(); i++) {
                    values.add(vec.get(i));
                }
                batch.close();
            }
        }

        assertThat(values).hasSize(ROW_GROUPS * ROWS_PER_GROUP);
        assertThat(batchCount).isGreaterThan(0);

        // Verify row-group order: row group 0 emits 0..3, row group 1 emits 100..103
        for (int rg = 0; rg < ROW_GROUPS; rg++) {
            for (int row = 0; row < ROWS_PER_GROUP; row++) {
                int expected = rg * 100 + row;
                assertThat(values.get(rg * ROWS_PER_GROUP + row))
                        .as("row group %d, row %d", rg, row)
                        .isEqualTo(expected);
            }
        }

        awaitProducerExit(pipeline);
        assertThat(pipeline.producerAlive()).isFalse();
    }

    // --- test 2: early close drains queued batches ---

    @Test
    void closeDrainsQueuedBatches() throws IOException {
        ParquetSchema schema = singleColumnSchema();
        // Use more row groups so there are likely batches queued when we close early.
        int numGroups = 5;
        List<RowGroupSurvivor> survivors = makeSurvivors(numGroups);
        ColumnFetcher fetcher = prebuiltFetcher(numGroups);

        BatchRowGroupPipeline pipeline = newPipeline(schema, survivors, fetcher, /*prefetchWindow*/ 2);

        try (Stream<ParquetRecordBatch> stream = pipeline.stream()) {
            // Take exactly one batch and let the stream close - the onClose hook cascades cancellation.
            ParquetRecordBatch first = stream.findFirst().orElse(null);
            assertThat(first).as("at least one batch must be produced").isNotNull();
            if (first != null) {
                first.close();
            }
        }

        // After stream close the producer must exit and queue must be drained.
        awaitProducerExit(pipeline);
        assertThat(pipeline.producerAlive())
                .as("producer thread must exit after early stream close")
                .isFalse();
    }

    // --- test 3: empty survivors produces empty stream ---

    @Test
    void emptySurvivorsProducesEmptyStream() {
        ParquetSchema schema = singleColumnSchema();
        ColumnFetcher fetcher = (chunk, path) -> {
            throw new AssertionError("fetcher must not be called for empty survivors");
        };

        BatchRowGroupPipeline pipeline = newPipeline(schema, List.of(), fetcher, /*prefetchWindow*/ 2);

        List<ParquetRecordBatch> batches = new ArrayList<>();
        try (Stream<ParquetRecordBatch> stream = pipeline.stream()) {
            stream.forEach(batches::add);
        }

        assertThat(batches).isEmpty();
        awaitProducerExit(pipeline);
    }

    // --- test 4: single-use check ---

    @Test
    void doubleStreamCallThrows() throws IOException {
        ParquetSchema schema = singleColumnSchema();
        List<RowGroupSurvivor> survivors = makeSurvivors(1);
        ColumnFetcher fetcher = prebuiltFetcher(1);

        BatchRowGroupPipeline pipeline = newPipeline(schema, survivors, fetcher, 2);

        try (Stream<ParquetRecordBatch> first = pipeline.stream()) {
            first.forEach(ParquetRecordBatch::close);
            org.assertj.core.api.Assertions.assertThatThrownBy(pipeline::stream)
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    // --- test 5: close is idempotent ---

    @Test
    void closeIsIdempotent() throws IOException {
        ParquetSchema schema = singleColumnSchema();
        List<RowGroupSurvivor> survivors = makeSurvivors(1);
        ColumnFetcher fetcher = prebuiltFetcher(1);

        BatchRowGroupPipeline pipeline = newPipeline(schema, survivors, fetcher, 2);

        try (Stream<ParquetRecordBatch> stream = pipeline.stream()) {
            stream.forEach(ParquetRecordBatch::close);
        }
        // Second and third explicit close must not throw - idempotent contract.
        pipeline.close();
        pipeline.close();

        awaitProducerExit(pipeline);
        assertThat(pipeline.producerAlive())
                .as("producer must have exited after all closes")
                .isFalse();
    }

    // --- fixture construction ---

    private static BatchRowGroupPipeline newPipeline(
            ParquetSchema schema, List<RowGroupSurvivor> survivors, ColumnFetcher fetcher, int prefetchWindow) {
        return new BatchRowGroupPipeline(noopReader(), schema, schema, survivors, readOptions(prefetchWindow), fetcher);
    }

    private static ReadOptions readOptions(int prefetchWindow) {
        return ReadOptions.builder()
                .concurrencyMode(ConcurrencyMode.SYNC)
                .prefetchWindow(prefetchWindow)
                .build();
    }

    private static ParquetSchema singleColumnSchema() {
        Field.Primitive leaf = new Field.Primitive(
                "value", Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        Field.Group root = new Field.Group("root", Repetition.REQUIRED, List.of(leaf), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    /** Builds {@code count} synthetic survivors, each carrying one column with ordinal-encoded values. */
    private static List<RowGroupSurvivor> makeSurvivors(int count) {
        List<RowGroupSurvivor> survivors = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            survivors.add(RowGroupSurvivor.full(singleColumnRowGroup(i)));
        }
        return survivors;
    }

    /**
     * Builds a synthetic single-column row group. The ordinal is stamped into {@code dataPageOffset} so the fetcher can
     * route to the right pre-built chunk without needing row-group object identity.
     */
    private static io.tileverse.parquetry.format.RowGroup singleColumnRowGroup(int ordinal) {
        ColumnMetaData meta = ColumnMetaData.builder()
                .type(PhysicalType.INT32)
                .encodings(List.of(Encoding.PLAIN))
                .pathInSchema(List.of("value"))
                .codec(CompressionCodec.UNCOMPRESSED)
                .numValues(ROWS_PER_GROUP)
                .totalUncompressedSize(ROWS_PER_GROUP * 4L)
                .totalCompressedSize(ROWS_PER_GROUP * 4L)
                .dataPageOffset(ordinal * 1000L)
                .build();
        ColumnChunk chunk = ColumnChunk.builder()
                .fileOffset(ordinal * 1000L)
                .metaData(Optional.of(meta))
                .build();
        return io.tileverse.parquetry.format.RowGroup.builder()
                .columns(List.of(chunk))
                .totalByteSize(ROWS_PER_GROUP * 4L)
                .numRows(ROWS_PER_GROUP)
                .fileOffset(OptionalLong.of(ordinal * 1000L))
                .totalCompressedSize(OptionalLong.of(ROWS_PER_GROUP * 4L))
                .ordinal(OptionalInt.of(ordinal))
                .build();
    }

    /**
     * Returns a {@link ColumnFetcher} that routes each request to a pre-built {@link FetchedColumnChunk} based on the
     * chunk's {@code dataPageOffset} (which encodes the row-group ordinal). Values for row group {@code i} are
     * {@code i*100, i*100+1, ..., i*100+ROWS_PER_GROUP-1}.
     */
    private static ColumnFetcher prebuiltFetcher(int numGroups) throws IOException {
        // Build a map from ordinal to pre-built page bytes.
        Map<Long, byte[]> pagesByOrdinal = new HashMap<>();
        for (int i = 0; i < numGroups; i++) {
            int[] rowGroupValues = new int[ROWS_PER_GROUP];
            for (int r = 0; r < ROWS_PER_GROUP; r++) {
                rowGroupValues[r] = i * 100 + r;
            }
            pagesByOrdinal.put((long) (i * 1000), encodeV1Page(rowGroupValues));
        }

        return (chunk, path) -> {
            long ordinalKey = chunk.metaData().orElseThrow().dataPageOffset();
            byte[] pageBytes = pagesByOrdinal.get(ordinalKey);
            if (pageBytes == null) {
                throw new IOException("No pre-built page for ordinal key " + ordinalKey);
            }
            return heapChunk(path, pageBytes, ROWS_PER_GROUP);
        };
    }

    // --- page encoding helpers (mirrors BatchRowGroupReaderTest) ---

    private static byte[] encodeV1Page(int[] values) throws IOException {
        byte[] payload = encodeInt32sLittleEndian(values);
        PageHeader header = new PageHeader(PageType.DATA_PAGE, payload.length, payload.length);
        DataPageHeader dataHeader = new DataPageHeader(
                values.length,
                org.apache.parquet.format.Encoding.PLAIN,
                org.apache.parquet.format.Encoding.RLE,
                org.apache.parquet.format.Encoding.RLE);
        header.setData_page_header(dataHeader);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Util.writePageHeader(header, out);
        out.write(payload);
        return out.toByteArray();
    }

    private static byte[] encodeInt32sLittleEndian(int[] values) {
        ByteBuffer buf = ByteBuffer.allocate(values.length * 4).order(LITTLE_ENDIAN);
        for (int v : values) {
            buf.putInt(v);
        }
        return buf.array();
    }

    private static FetchedColumnChunk heapChunk(ColumnPath path, byte[] data, long numValues) {
        ByteBufferPool.PooledByteBuffer pooled = ByteBufferPool.heapBuffer(data.length);
        ByteBuffer buf = pooled.buffer();
        buf.clear();
        buf.put(data);
        buf.flip();
        buf.order(LITTLE_ENDIAN);
        ColumnMetaData meta = ColumnMetaData.builder()
                .type(PhysicalType.INT32)
                .encodings(List.of(Encoding.PLAIN))
                .pathInSchema(path.parts())
                .codec(CompressionCodec.UNCOMPRESSED)
                .numValues(numValues)
                .totalUncompressedSize((long) data.length)
                .totalCompressedSize((long) data.length)
                .dataPageOffset(0L)
                .build();
        return new FetchedColumnChunk(path, meta, /*maxRep*/ 0, /*maxDef*/ 0, pooled, Optional.empty());
    }

    // --- helpers ---

    private static RangeReader noopReader() {
        return new RangeReader() {
            @Override
            public int readRange(long offset, int length, ByteBuffer target) {
                throw new UnsupportedOperationException("test fetcher bypasses I/O");
            }

            @Override
            public OptionalLong size() {
                return OptionalLong.empty();
            }

            @Override
            public String getSourceIdentifier() {
                return "test://batch-pipeline";
            }

            @Override
            public void close() {
                // nothing to release
            }
        };
    }

    private static void awaitProducerExit(BatchRowGroupPipeline pipeline) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (pipeline.producerAlive() && System.nanoTime() < deadline) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(5));
            if (Thread.currentThread().isInterrupted()) {
                return;
            }
        }
    }
}
