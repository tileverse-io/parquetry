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
package io.tileverse.parquetry.benchmarks;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.BooleanVector;
import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DoubleVector;
import io.tileverse.parquetry.columnar.FixedLenBinaryVector;
import io.tileverse.parquetry.columnar.FloatVector;
import io.tileverse.parquetry.columnar.IntVector;
import io.tileverse.parquetry.columnar.LongVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Isolates the per-column decode hot path: the cost of turning one Parquet column chunk back into typed
 * {@link ColumnVector}s, with no filtering, projection narrowing, or record assembly in the way.
 *
 * <p>The fixture is a single-column file read in full through {@code readBatches(ALWAYS_TRUE, ALL, DEFAULTS)}; the
 * benchmark folds every decoded value into a sink to defeat dead-code elimination. The axes cover the dimensions the
 * decoder branches on:
 *
 * <ul>
 *   <li>{@code scenario} pairs a primitive {@link PrimitiveKind} with a forced on-disk encoding. PLAIN, DICTIONARY,
 *       DELTA (delta-binary-packed for integers, delta-byte-array for binary), and BYTE_STREAM_SPLIT each drive a
 *       distinct decoder; only the pairings a kind actually supports are listed, to avoid a write that no encoder can
 *       satisfy.
 *   <li>{@code nullable} selects an all-present column (the PLAIN fast path can slice values straight from the page)
 *       versus a column with roughly one null in ten (which forces the null-positioning spread and a validity bitmap).
 * </ul>
 *
 * <p>This is the one microbenchmark that times raw decode rather than the filter, pruning, or spatial machinery around
 * it; it is the harness for confirming a change to the decode path stays neutral on throughput. The scenario matrix is
 * wide on purpose - pin {@code -p scenario=...} and {@code -p nullable=...} to time the slice a change touches:
 *
 * <pre>{@code
 * ./mvnw -Pbenchmarks -pl :parquetry-benchmarks -am package
 * java --enable-preview --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow \
 *   -jar internal/parquetry-benchmarks/target/benchmarks.jar DecodeBenchmark \
 *   -p scenario=INT64_PLAIN,DOUBLE_PLAIN -p nullable=false
 * }</pre>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(1)
public class DecodeBenchmark {

    private static final int FIXED_WIDTH = 16;
    // Value domain size; bounds the dictionary's cardinality so the DICTIONARY scenarios decode a realistic index width
    // rather than a degenerate one-entry-per-row dictionary.
    private static final int DISTINCT_VALUES = 1_024;
    // One null in every NULL_STRIDE rows on the nullable arm.
    private static final int NULL_STRIDE = 10;
    private static final String COLUMN = "v";

    /** A primitive kind paired with an on-disk encoding the writer can produce for it. */
    public enum Scenario {
        INT32_PLAIN(PrimitiveKind.INT32, WriteOptions.EncodingPolicy.FORCE_PLAIN),
        INT32_DICTIONARY(PrimitiveKind.INT32, WriteOptions.EncodingPolicy.FORCE_DICTIONARY),
        INT32_DELTA(PrimitiveKind.INT32, WriteOptions.EncodingPolicy.FORCE_DELTA),
        INT64_PLAIN(PrimitiveKind.INT64, WriteOptions.EncodingPolicy.FORCE_PLAIN),
        INT64_DICTIONARY(PrimitiveKind.INT64, WriteOptions.EncodingPolicy.FORCE_DICTIONARY),
        INT64_DELTA(PrimitiveKind.INT64, WriteOptions.EncodingPolicy.FORCE_DELTA),
        FLOAT_PLAIN(PrimitiveKind.FLOAT, WriteOptions.EncodingPolicy.FORCE_PLAIN),
        FLOAT_BYTE_STREAM_SPLIT(PrimitiveKind.FLOAT, WriteOptions.EncodingPolicy.FORCE_BYTE_STREAM_SPLIT),
        DOUBLE_PLAIN(PrimitiveKind.DOUBLE, WriteOptions.EncodingPolicy.FORCE_PLAIN),
        DOUBLE_BYTE_STREAM_SPLIT(PrimitiveKind.DOUBLE, WriteOptions.EncodingPolicy.FORCE_BYTE_STREAM_SPLIT),
        BOOLEAN_PLAIN(PrimitiveKind.BOOLEAN, WriteOptions.EncodingPolicy.FORCE_PLAIN),
        BINARY_PLAIN(PrimitiveKind.BYTE_ARRAY, WriteOptions.EncodingPolicy.FORCE_PLAIN),
        BINARY_DICTIONARY(PrimitiveKind.BYTE_ARRAY, WriteOptions.EncodingPolicy.FORCE_DICTIONARY),
        BINARY_DELTA(PrimitiveKind.BYTE_ARRAY, WriteOptions.EncodingPolicy.FORCE_DELTA),
        FLBA_PLAIN(PrimitiveKind.FIXED_LEN_BYTE_ARRAY, WriteOptions.EncodingPolicy.FORCE_PLAIN),
        FLBA_DICTIONARY(PrimitiveKind.FIXED_LEN_BYTE_ARRAY, WriteOptions.EncodingPolicy.FORCE_DICTIONARY);

        private final PrimitiveKind kind;
        private final WriteOptions.EncodingPolicy policy;

        Scenario(PrimitiveKind kind, WriteOptions.EncodingPolicy policy) {
            this.kind = kind;
            this.policy = policy;
        }
    }

    @Param({
        "INT32_PLAIN",
        "INT32_DICTIONARY",
        "INT32_DELTA",
        "INT64_PLAIN",
        "INT64_DICTIONARY",
        "INT64_DELTA",
        "FLOAT_PLAIN",
        "FLOAT_BYTE_STREAM_SPLIT",
        "DOUBLE_PLAIN",
        "DOUBLE_BYTE_STREAM_SPLIT",
        "BOOLEAN_PLAIN",
        "BINARY_PLAIN",
        "BINARY_DICTIONARY",
        "BINARY_DELTA",
        "FLBA_PLAIN",
        "FLBA_DICTIONARY"
    })
    private Scenario scenario;

    @Param({"false", "true"})
    private boolean nullable;

    /**
     * Shrinks the fixture to a few thousand rows for the CI sanity run that only checks the benchmark still runs. The
     * default keeps the full measurement workload; it is not a measurement mode.
     */
    @Param({"false"})
    private boolean smoke;

    private int rows;
    private Path workDir;
    private SyntheticParquet.OpenDataset open;

    @Setup(Level.Trial)
    public void setUp() throws IOException {
        rows = smoke ? 16_384 : 1_000_000;
        workDir = Files.createTempDirectory("parquetry-bench-decode-");
        Path file = workDir.resolve("decode.parquet");
        writeColumn(file);
        open = SyntheticParquet.open(file);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        open.close();
        SyntheticParquet.deleteRecursively(workDir);
    }

    @Benchmark
    public long decode() {
        long[] sink = {0L};
        try (Stream<ParquetRecordBatch> batches =
                open.reader().readBatches(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
            for (ParquetRecordBatch batch : (Iterable<ParquetRecordBatch>) batches::iterator) {
                try (batch) {
                    sink[0] += foldBatch(batch);
                }
            }
        }
        return sink[0];
    }

    /** Folds every value of the batch's single column into a checksum, forcing each decoded value to be read. */
    private long foldBatch(ParquetRecordBatch batch) {
        long sink = 0L;
        for (ColumnVector column : batch.columns().values()) {
            sink += foldVector(column);
        }
        return sink;
    }

    private long foldVector(ColumnVector vector) {
        return switch (vector) {
            case IntVector ints -> foldInts(ints);
            case LongVector longs -> foldLongs(longs);
            case FloatVector floats -> foldFloats(floats);
            case DoubleVector doubles -> foldDoubles(doubles);
            case BooleanVector booleans -> foldBooleans(booleans);
            case BinaryVector binaries -> foldBinaries(binaries);
            case FixedLenBinaryVector fixed -> foldFixedLenBinaries(fixed);
            default -> throw new IllegalStateException("unexpected vector for a flat column: " + vector.getClass());
        };
    }

    private long foldInts(IntVector vector) {
        long sink = 0L;
        int size = vector.size();
        for (int row = 0; row < size; row++) {
            if (!vector.isNull(row)) {
                sink += vector.valueAt(row);
            }
        }
        return sink;
    }

    private long foldLongs(LongVector vector) {
        long sink = 0L;
        int size = vector.size();
        for (int row = 0; row < size; row++) {
            if (!vector.isNull(row)) {
                sink += vector.valueAt(row);
            }
        }
        return sink;
    }

    private long foldFloats(FloatVector vector) {
        long sink = 0L;
        int size = vector.size();
        for (int row = 0; row < size; row++) {
            if (!vector.isNull(row)) {
                sink += (long) vector.valueAt(row);
            }
        }
        return sink;
    }

    private long foldDoubles(DoubleVector vector) {
        long sink = 0L;
        int size = vector.size();
        for (int row = 0; row < size; row++) {
            if (!vector.isNull(row)) {
                sink += (long) vector.valueAt(row);
            }
        }
        return sink;
    }

    private long foldBooleans(BooleanVector vector) {
        long sink = 0L;
        int size = vector.size();
        for (int row = 0; row < size; row++) {
            if (!vector.isNull(row) && vector.valueAt(row)) {
                sink++;
            }
        }
        return sink;
    }

    private long foldBinaries(BinaryVector vector) {
        long sink = 0L;
        int size = vector.size();
        for (int row = 0; row < size; row++) {
            MemorySegment value = vector.get(row);
            if (value != null) {
                sink += value.byteSize();
            }
        }
        return sink;
    }

    private long foldFixedLenBinaries(FixedLenBinaryVector vector) {
        long sink = 0L;
        int size = vector.size();
        for (int row = 0; row < size; row++) {
            MemorySegment value = vector.get(row);
            if (value != null) {
                sink += value.byteSize();
            }
        }
        return sink;
    }

    // --- fixture writing ---

    private void writeColumn(Path file) throws IOException {
        ParquetSchema schema = singleColumnSchema(scenario.kind);
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, writeOptions())) {
            ParquetRecordBatchBuilder appender = writer.appender();
            for (int i = 0; i < rows; i++) {
                if (nullable && i % NULL_STRIDE == 0) {
                    appender.setNull(0);
                } else {
                    stageValue(appender, i % DISTINCT_VALUES);
                }
                appender.endRow();
            }
            appender.flush();
        }
    }

    private void stageValue(ParquetRecordBatchBuilder appender, int base) {
        switch (scenario.kind) {
            case INT32 -> appender.setInt(0, base);
            case INT64 -> appender.setLong(0, base);
            case FLOAT -> appender.setFloat(0, base);
            case DOUBLE -> appender.setDouble(0, base);
            case BOOLEAN -> appender.setBoolean(0, (base & 1) == 0);
            case BYTE_ARRAY -> appender.setBinary(0, utf8(Integer.toString(base)));
            case FIXED_LEN_BYTE_ARRAY -> appender.setBinary(0, fixedWidthValue(base));
            case INT96 -> throw new IllegalStateException("INT96 is not a writable decode scenario");
        }
    }

    private static MemorySegment utf8(String value) {
        return MemorySegment.ofArray(value.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * A deterministic {@code FIXED_WIDTH}-byte value derived from {@code base}; distinct {@code base} yield distinct
     * slots.
     */
    private static MemorySegment fixedWidthValue(int base) {
        byte[] bytes = new byte[FIXED_WIDTH];
        for (int k = 0; k < FIXED_WIDTH; k++) {
            bytes[k] = (byte) (base + k);
        }
        return MemorySegment.ofArray(bytes);
    }

    private WriteOptions writeOptions() {
        return WriteOptions.builder()
                .tempDir(workDir)
                .rowGroupSize(WriteOptions.RowGroupSize.rows(rows + 1L))
                .pageValueLimit(8_192)
                .encodingPolicy(COLUMN, scenario.policy)
                .build();
    }

    private ParquetSchema singleColumnSchema(PrimitiveKind kind) {
        Repetition repetition = nullable ? Repetition.OPTIONAL : Repetition.REQUIRED;
        OptionalInt typeLength =
                kind == PrimitiveKind.FIXED_LEN_BYTE_ARRAY ? OptionalInt.of(FIXED_WIDTH) : OptionalInt.empty();
        SchemaNode.Primitive leaf =
                new SchemaNode.Primitive(COLUMN, repetition, kind, typeLength, Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(leaf), Optional.empty(), -1);
        return new ParquetSchema(root);
    }
}
