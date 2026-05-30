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
package io.tileverse.parquetry.benchmarks;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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

import io.tileverse.parquetry.data.ParquetReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;

/**
 * Compares the optimized {@link ParquetReader#count(Predicate, ReadOptions)} path against the
 * {@code read(predicate).count()} baseline across the count paths a row count can take.
 *
 * <p>The fixture is a sorted {@code id INT64 + value DOUBLE} table written across several row groups. The {@code path}
 * parameter selects a predicate that drives the count down a distinct path:
 *
 * <ul>
 *   <li>{@code ALWAYS_TRUE} sums per-row-group counts from metadata without decoding any column.
 *   <li>{@code MATCHED} proves every row group matches (the sorted, non-null {@code id >= 0}) and again counts from
 *       metadata.
 *   <li>{@code ELIMINATED} prunes every row group (an {@code id} above every group's max), counting zero rows.
 *   <li>{@code RESIDUAL} leaves the middle row groups undecided ({@code id > rows / 2}), forcing record-level
 *       evaluation on those groups while metadata settles the rest.
 *   <li>{@code IS_NOT_NULL} counts all rows of a non-null column, which the metadata null counts settle without a
 *       decode.
 * </ul>
 *
 * <p>The optimized benchmark calls {@link ParquetReader#count(Predicate, ReadOptions)} directly; the baseline opens the
 * same predicate as a record stream and counts its elements. There are no assertions: this is a regression guard and a
 * quantifier for the optimized path's advantage, not a correctness test.
 *
 * <pre>{@code
 * ./mvnw -Pbenchmarks -pl :parquetry-benchmarks -am package
 * java --enable-preview --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow \
 *   -jar internal/parquetry-benchmarks/target/benchmarks.jar CountBenchmark
 * }</pre>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
public class CountBenchmark {

    /** Selects the predicate, and through it the count path each configuration exercises. */
    public enum CountPath {
        ALWAYS_TRUE,
        MATCHED,
        ELIMINATED,
        RESIDUAL,
        IS_NOT_NULL
    }

    @Param({"ALWAYS_TRUE", "MATCHED", "ELIMINATED", "RESIDUAL", "IS_NOT_NULL"})
    private CountPath path;

    /**
     * Shrinks the fixture to a few thousand rows across a handful of row groups for the CI sanity run that only checks
     * the benchmark still runs. The default keeps the full measurement workload.
     */
    @Param({"false"})
    private boolean smoke;

    private Path workDir;
    private ByteRangeSource source;
    private ParquetReader reader;
    private Predicate predicate;
    private ReadOptions options;

    @Setup(Level.Trial)
    public void setUp() throws IOException {
        int rows = smoke ? 4_000 : 1_000_000;
        int rowsPerGroup = smoke ? 1_000 : 100_000;

        workDir = Files.createTempDirectory("parquetry-bench-count-");
        Path file = workDir.resolve("count.parquet");
        SyntheticParquet.writeIdValueFile(file, writeOptions(rowsPerGroup), SyntheticParquet.sortedIds(rows));

        source = ByteRangeSource.ofFile(file);
        reader = ParquetReader.open(source);
        predicate = predicateFor(path, rows);
        options = ReadOptions.builder().build();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        source.close();
        SyntheticParquet.deleteRecursively(workDir);
    }

    @Benchmark
    public long optimizedCount() {
        return reader.count(predicate, options);
    }

    @Benchmark
    public long readCountBaseline() {
        try (Stream<ParquetRecord> rows = reader.read(predicate, Projection.ALL, options)) {
            return rows.count();
        }
    }

    private static Predicate predicateFor(CountPath path, int rows) {
        return switch (path) {
            case ALWAYS_TRUE -> Predicate.ALWAYS_TRUE;
            case MATCHED -> Pred.col(SyntheticParquet.ID).gtEq(0L);
            case ELIMINATED -> Pred.col(SyntheticParquet.ID).gt(rows + 1_000L);
            case RESIDUAL -> Pred.col(SyntheticParquet.ID).gt((long) rows / 2);
            case IS_NOT_NULL -> Pred.col(SyntheticParquet.ID).isNotNull();
        };
    }

    private WriteOptions writeOptions(int rowsPerGroup) {
        return WriteOptions.builder()
                .tempDir(workDir)
                .rowGroupSize(WriteOptions.RowGroupSize.rows(rowsPerGroup))
                .build();
    }
}
