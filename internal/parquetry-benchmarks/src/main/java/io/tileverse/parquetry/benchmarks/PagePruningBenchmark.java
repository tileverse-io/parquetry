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

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.record.ParquetRecord;

/**
 * Measures what column-index page pruning buys a filtered read.
 *
 * <p>The fixture is one Parquet file with a single row group and many small pages, holding an {@code id} key column and
 * a {@code value} payload column. A selective band predicate on {@code id} matches roughly one percent of the rows. The
 * two axes isolate the optimization and show its data dependence:
 *
 * <ul>
 *   <li>{@code useColumnIndex} toggles the COLUMN_INDEX filter tier (the page-pruning path). With it off, every page is
 *       decoded; with it on, only the pages whose column-index bounds overlap the predicate are decoded.
 *   <li>{@code layout} controls how {@code id} is ordered on disk. {@code SORTED} clusters the matching rows into a
 *       handful of adjacent pages, leaving pruning to skip almost all decode. {@code SHUFFLED} spreads them across
 *       every page; each page's bounds then span the whole domain and pruning can skip nothing - the honest no-win
 *       baseline.
 * </ul>
 *
 * <p>Both arms return identical results (page pruning never drops a matching row); the difference is decode work, hence
 * time. Run it with the shaded jar built under the {@code benchmarks} profile:
 *
 * <pre>{@code
 * ./mvnw -Pbenchmarks -pl :parquetry-benchmarks -am package
 * java --enable-preview --enable-native-access=ALL-UNNAMED \
 *   -jar internal/parquetry-benchmarks/target/benchmarks.jar PagePruningBenchmark
 * }</pre>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(
        value = 1,
        jvmArgsAppend = {"--enable-preview", "--enable-native-access=ALL-UNNAMED"})
public class PagePruningBenchmark {

    private static final int ROWS = 1_000_000;
    private static final int PAGE_VALUES = 2_048;
    private static final int BAND = ROWS / 100;
    private static final long PREDICATE_LOW = (ROWS - BAND) / 2L;
    private static final long PREDICATE_HIGH = PREDICATE_LOW + BAND - 1L;
    private static final long SHUFFLE_SEED = 42L;

    /** Disk order of the {@code id} column: clustered (best case for pruning) or scattered (no pruning possible). */
    public enum Layout {
        SORTED,
        SHUFFLED
    }

    @Param({"SORTED", "SHUFFLED"})
    private Layout layout;

    @Param({"true", "false"})
    private boolean useColumnIndex;

    private Path workDir;
    private SyntheticParquet.OpenDataset open;
    private Predicate predicate;
    private Projection projection;
    private ReadOptions options;

    @Setup(Level.Trial)
    public void setUp() throws IOException {
        workDir = Files.createTempDirectory("parquetry-bench-page-pruning-");
        Path file = workDir.resolve("page-pruning.parquet");
        SyntheticParquet.writeIdValueFile(file, writeOptions(), idsForLayout());
        open = SyntheticParquet.open(file);
        predicate = Pred.col(SyntheticParquet.ID)
                .gtEq(PREDICATE_LOW)
                .and(Pred.col(SyntheticParquet.ID).ltEq(PREDICATE_HIGH));
        projection = Projection.ALL;
        options = ReadOptions.builder().useColumnIndexFilter(useColumnIndex).build();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        open.close();
        SyntheticParquet.deleteRecursively(workDir);
    }

    @Benchmark
    public double filteredRead() {
        try (Stream<ParquetRecord> rows = open.dataset().read(predicate, projection, options)) {
            return rows.mapToDouble(record -> record.getDouble(SyntheticParquet.VALUE))
                    .sum();
        }
    }

    private long[] idsForLayout() {
        return layout == Layout.SORTED
                ? SyntheticParquet.sortedIds(ROWS)
                : SyntheticParquet.shuffledIds(ROWS, SHUFFLE_SEED);
    }

    private WriteOptions writeOptions() {
        return WriteOptions.builder()
                .tempDir(workDir)
                .rowGroupSize(WriteOptions.RowGroupSize.rows(ROWS + 1L))
                .pageValueLimit(PAGE_VALUES)
                .encodingPolicy("id", WriteOptions.EncodingPolicy.FORCE_PLAIN)
                .encodingPolicy("value", WriteOptions.EncodingPolicy.FORCE_PLAIN)
                .build();
    }
}
