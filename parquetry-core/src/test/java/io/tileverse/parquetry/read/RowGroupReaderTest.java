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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.CompressionCodec;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.page.LevelDecoder;
import io.tileverse.parquetry.page.PlainBinaryDecoder;
import io.tileverse.parquetry.page.PlainDoubleDecoder;
import io.tileverse.parquetry.page.PlainInt32Decoder;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.Field;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;

import io.tileverse.io.ByteBufferPool;

/**
 * Exercises the {@link RowGroupReader} orchestration contract on a synthetic 3-column row group: SYNC vs
 * {@code FAN_OUT_ONLY} must produce identical results, fan-out forks one subtask per projected column, and every
 * borrowed pooled buffer is returned to the pool when the stream closes.
 *
 * <p>Real page-decoding I/O is not exercised here; that wiring lands later. The test uses the package-private
 * {@code RowGroupReader} constructor to supply a column-reader factory backed by {@code BasicColumnReader} fed from
 * pre-built byte buffers, which lets us verify the streaming/pooled-buffer contract without inventing a Parquet write
 * path.
 */
class RowGroupReaderTest {

    private static final int ROW_COUNT = 3;

    private static final ColumnPath YEAR_PATH = ColumnPath.of("year");
    private static final ColumnPath COUNTRY_PATH = ColumnPath.of("country");
    private static final ColumnPath VALUE_PATH = ColumnPath.of("value");

    @Test
    void syncAndFanOutProduceIdenticalRecords() {
        ParquetSchema schema = threeColumnSchema();
        RowGroup rowGroup = threeColumnRowGroup();
        ByteBufferPool pool = freshPool();

        List<ParquetRecord> sync = readAllWithMode(schema, rowGroup, pool, ConcurrencyMode.SYNC);
        List<ParquetRecord> fanOut = readAllWithMode(schema, rowGroup, pool, ConcurrencyMode.FAN_OUT_ONLY);

        assertThat(sync).hasSize(ROW_COUNT);
        assertThat(fanOut).hasSize(ROW_COUNT);

        assertRecordsEqual(sync, fanOut);
        assertExpectedRows(sync);
    }

    @Test
    void fanOutForksOneSubtaskPerProjectedColumn() {
        ParquetSchema schema = threeColumnSchema();
        RowGroup rowGroup = threeColumnRowGroup();
        ByteBufferPool pool = freshPool();
        AtomicInteger fetchCount = new AtomicInteger();

        ColumnFetcher counting = countingFetcher(pool, fetchCount);
        RowGroupReader reader = new RowGroupReader(
                schema,
                schema,
                rowGroup,
                readOptions(pool, ConcurrencyMode.FAN_OUT_ONLY),
                counting,
                fixedRowsFactory());
        try (Stream<ParquetRecord> stream = reader.read(Materializer.defaultRecord())) {
            stream.forEach(_ -> {});
        }

        assertThat(fetchCount.get())
                .as("fan-out should fork one subtask per projected leaf")
                .isEqualTo(3);
    }

    @Test
    void streamCloseReturnsEveryBorrowedBuffer() {
        ParquetSchema schema = threeColumnSchema();
        RowGroup rowGroup = threeColumnRowGroup();
        ByteBufferPool pool = freshPool();

        RowGroupReader reader = new RowGroupReader(
                schema,
                schema,
                rowGroup,
                readOptions(pool, ConcurrencyMode.FAN_OUT_ONLY),
                borrowingFetcher(pool),
                fixedRowsFactory());
        try (Stream<ParquetRecord> stream = reader.read(Materializer.defaultRecord())) {
            stream.forEach(_ -> {});
        }

        assertThat(outstandingBorrows(pool))
                .as("every borrowed PooledByteBuffer must be closed when the stream closes")
                .isZero();
    }

    @Test
    void earlyStreamCloseStillReleasesBuffers() {
        ParquetSchema schema = threeColumnSchema();
        RowGroup rowGroup = threeColumnRowGroup();
        ByteBufferPool pool = freshPool();

        RowGroupReader reader = new RowGroupReader(
                schema,
                schema,
                rowGroup,
                readOptions(pool, ConcurrencyMode.FAN_OUT_ONLY),
                borrowingFetcher(pool),
                fixedRowsFactory());
        try (Stream<ParquetRecord> stream = reader.read(Materializer.defaultRecord())) {
            // consume only one record then bail out via try-with-resources close
            stream.limit(1).forEach(_ -> {});
        }

        assertThat(outstandingBorrows(pool))
                .as("buffers must be returned even when the consumer aborts mid-iteration")
                .isZero();
    }

    @Test
    void fetchFailurePropagatesAndReleasesAlreadyBorrowedBuffers() {
        ParquetSchema schema = threeColumnSchema();
        RowGroup rowGroup = threeColumnRowGroup();
        ByteBufferPool pool = freshPool();
        AtomicInteger fetchCount = new AtomicInteger();

        ColumnFetcher delegate = borrowingFetcher(pool);
        ColumnFetcher failingOnValue = (chunk, path) -> {
            fetchCount.incrementAndGet();
            if (path.equals(VALUE_PATH)) {
                throw new IOException("synthetic value fetch failure");
            }
            return delegate.fetch(chunk, path);
        };

        // SYNC mode fetches columns in declared order (year, country, value), guaranteeing that
        // year and country have already borrowed buffers by the time the synthetic failure on value
        // fires; the pool-drain assertion below is then load-bearing rather than vacuously zero.
        RowGroupReader reader = new RowGroupReader(
                schema, schema, rowGroup, readOptions(pool, ConcurrencyMode.SYNC), failingOnValue, fixedRowsFactory());
        Materializer<ParquetRecord> materializer = Materializer.defaultRecord();

        assertThatThrownBy(() -> reader.read(materializer))
                .isInstanceOf(UncheckedIOException.class)
                .hasMessageContaining("Column fetch failed");

        assertThat(fetchCount.get())
                .as("year and country must be fetched before value fails")
                .isEqualTo(3);
        assertThat(outstandingBorrows(pool))
                .as("buffers must be returned even when a fetch throws")
                .isZero();
    }

    // --- fixtures ---

    private static ParquetSchema threeColumnSchema() {
        return new ParquetSchema(new Field.Group(
                "root",
                Repetition.REQUIRED,
                List.of(
                        new Field.Primitive(
                                "year",
                                Repetition.REQUIRED,
                                PrimitiveKind.INT32,
                                OptionalInt.empty(),
                                Optional.empty(),
                                -1),
                        new Field.Primitive(
                                "country",
                                Repetition.REQUIRED,
                                PrimitiveKind.BYTE_ARRAY,
                                OptionalInt.empty(),
                                Optional.empty(),
                                -1),
                        new Field.Primitive(
                                "value",
                                Repetition.REQUIRED,
                                PrimitiveKind.DOUBLE,
                                OptionalInt.empty(),
                                Optional.empty(),
                                -1)),
                Optional.empty(),
                -1));
    }

    private static RowGroup threeColumnRowGroup() {
        ColumnMetaData yearMeta = primitiveMetadata(PhysicalType.INT32, List.of("year"), ROW_COUNT);
        ColumnMetaData countryMeta = primitiveMetadata(PhysicalType.BYTE_ARRAY, List.of("country"), ROW_COUNT);
        ColumnMetaData valueMeta = primitiveMetadata(PhysicalType.DOUBLE, List.of("value"), ROW_COUNT);
        List<ColumnChunk> columns = List.of(
                columnChunk(yearMeta, /*fileOffset*/ 0L),
                columnChunk(countryMeta, /*fileOffset*/ 100L),
                columnChunk(valueMeta, /*fileOffset*/ 200L));
        return RowGroup.builder()
                .columns(columns)
                .totalByteSize(300L)
                .numRows(ROW_COUNT)
                .fileOffset(OptionalLong.of(0L))
                .totalCompressedSize(OptionalLong.of(300L))
                .build();
    }

    private static ColumnChunk columnChunk(ColumnMetaData meta, long fileOffset) {
        return ColumnChunk.builder()
                .fileOffset(fileOffset)
                .metaData(Optional.of(meta))
                .build();
    }

    private static ColumnMetaData primitiveMetadata(PhysicalType type, List<String> pathInSchema, long numValues) {
        return ColumnMetaData.builder()
                .type(type)
                .encodings(List.of(Encoding.PLAIN))
                .pathInSchema(pathInSchema)
                .codec(CompressionCodec.UNCOMPRESSED)
                .numValues(numValues)
                .totalUncompressedSize(100L)
                .totalCompressedSize(100L)
                .dataPageOffset(0L)
                .build();
    }

    // --- expected row values ---

    private static int[] expectedYears() {
        return new int[] {2024, 2025, 2026};
    }

    private static String[] expectedCountries() {
        return new String[] {"AR", "BR", "CL"};
    }

    private static double[] expectedValues() {
        return new double[] {1.5, 2.5, 3.5};
    }

    // --- fetcher stubs ---

    /**
     * Borrows one tiny heap buffer per call (4 bytes is enough; the buffer's content does not matter because the test
     * column-reader factory drives {@code BasicColumnReader}s from pre-built {@code PlainXxxDecoder}s). Borrowing from
     * the {@code pool} is the load-bearing behavior under test: closing the resulting {@link FetchedColumnChunk}
     * returns the buffer.
     */
    private static ColumnFetcher borrowingFetcher(ByteBufferPool pool) {
        return (chunk, path) -> new FetchedColumnChunk(
                path, chunk.metaData().orElseThrow(), /*maxRep*/ 0, /*maxDef*/ 0, pool.borrowHeap(4), Optional.empty());
    }

    private static ColumnFetcher countingFetcher(ByteBufferPool pool, AtomicInteger counter) {
        ColumnFetcher delegate = borrowingFetcher(pool);
        return (chunk, path) -> {
            counter.incrementAndGet();
            return delegate.fetch(chunk, path);
        };
    }

    // --- column-reader factory backed by pre-built decoders ---

    /**
     * Builds {@link BasicColumnReader}s that yield the expected row values from in-test page buffers, independent of
     * the bytes the fetcher placed in the pooled buffer. This is the orchestration-test scope shrinkage: we test
     * orchestration (fork count, ordering, close lifecycle, pool returns) without re-implementing the page cursor.
     */
    private static BiFunction<FetchedColumnChunk, Field.Primitive, ColumnReader> fixedRowsFactory() {
        return (chunk, leaf) -> switch (leaf.kind()) {
            case INT32 -> intReader(chunk.path(), expectedYears());
            case BYTE_ARRAY -> binaryReader(chunk.path(), expectedCountries());
            case DOUBLE -> doubleReader(chunk.path(), expectedValues());
            default -> throw new IllegalStateException("unexpected kind " + leaf.kind());
        };
    }

    private static ColumnReader intReader(ColumnPath path, int[] values) {
        ByteBuffer page = ByteBuffer.allocate(values.length * 4).order(ByteOrder.LITTLE_ENDIAN);
        for (int v : values) {
            page.putInt(v);
        }
        page.flip();
        PlainInt32Decoder decoder = new PlainInt32Decoder();
        decoder.load(page, values.length);
        return new BasicColumnReader(path, 0, 0, new LevelDecoder(0), new LevelDecoder(0), decoder, values.length);
    }

    private static ColumnReader binaryReader(ColumnPath path, String[] values) {
        int capacity = 0;
        for (String s : values) {
            capacity += 4 + s.getBytes(StandardCharsets.UTF_8).length;
        }
        ByteBuffer page = ByteBuffer.allocate(capacity).order(ByteOrder.LITTLE_ENDIAN);
        for (String s : values) {
            byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
            page.putInt(bytes.length);
            page.put(bytes);
        }
        page.flip();
        PlainBinaryDecoder decoder = new PlainBinaryDecoder();
        decoder.load(page, values.length);
        return new BasicColumnReader(path, 0, 0, new LevelDecoder(0), new LevelDecoder(0), decoder, values.length);
    }

    private static ColumnReader doubleReader(ColumnPath path, double[] values) {
        ByteBuffer page = ByteBuffer.allocate(values.length * 8).order(ByteOrder.LITTLE_ENDIAN);
        for (double v : values) {
            page.putDouble(v);
        }
        page.flip();
        PlainDoubleDecoder decoder = new PlainDoubleDecoder();
        decoder.load(page, values.length);
        return new BasicColumnReader(path, 0, 0, new LevelDecoder(0), new LevelDecoder(0), decoder, values.length);
    }

    // --- helpers ---

    private static ByteBufferPool freshPool() {
        // Fresh pool so PoolStatistics reflects this test's borrows only, not the global default pool's history.
        return new ByteBufferPool();
    }

    private static long outstandingBorrows(ByteBufferPool pool) {
        ByteBufferPool.PoolStatistics stats = pool.getStatistics();
        return (stats.created() + stats.reused()) - (stats.returned() + stats.discarded());
    }

    private static ReadOptions readOptions(ByteBufferPool pool, ConcurrencyMode mode) {
        return ReadOptions.builder().concurrencyMode(mode).byteBufferPool(pool).build();
    }

    private static List<ParquetRecord> readAllWithMode(
            ParquetSchema schema, RowGroup rowGroup, ByteBufferPool pool, ConcurrencyMode mode) {
        RowGroupReader reader = new RowGroupReader(
                schema, schema, rowGroup, readOptions(pool, mode), borrowingFetcher(pool), fixedRowsFactory());
        List<ParquetRecord> collected = new ArrayList<>();
        try (Stream<ParquetRecord> stream = reader.read(Materializer.defaultRecord())) {
            stream.forEach(collected::add);
        }
        return Collections.unmodifiableList(collected);
    }

    private static void assertRecordsEqual(List<ParquetRecord> a, List<ParquetRecord> b) {
        assertThat(a).hasSameSizeAs(b);
        for (int i = 0; i < a.size(); i++) {
            ParquetRecord aRec = a.get(i);
            ParquetRecord bRec = b.get(i);
            assertThat(aRec.getInt(YEAR_PATH)).isEqualTo(bRec.getInt(YEAR_PATH));
            assertThat(stringAt(aRec, COUNTRY_PATH)).isEqualTo(stringAt(bRec, COUNTRY_PATH));
            assertThat(aRec.getDouble(VALUE_PATH)).isEqualTo(bRec.getDouble(VALUE_PATH));
        }
    }

    private static void assertExpectedRows(List<ParquetRecord> rows) {
        int[] years = expectedYears();
        String[] countries = expectedCountries();
        double[] values = expectedValues();
        for (int i = 0; i < ROW_COUNT; i++) {
            ParquetRecord parquetRecord = rows.get(i);
            assertThat(parquetRecord.getInt(YEAR_PATH)).isEqualTo(years[i]);
            assertThat(stringAt(parquetRecord, COUNTRY_PATH)).isEqualTo(countries[i]);
            assertThat(parquetRecord.getDouble(VALUE_PATH)).isEqualTo(values[i]);
        }
    }

    private static String stringAt(ParquetRecord parquetRecord, ColumnPath path) {
        return parquetRecord.getString(path);
    }
}
