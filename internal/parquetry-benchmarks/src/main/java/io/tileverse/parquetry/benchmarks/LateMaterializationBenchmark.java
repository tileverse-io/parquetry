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

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.record.ParquetRecord;

/**
 * Measures the impact of late materialization on filtered reads over a wide row.
 *
 * <p>The fixture is a wide Parquet table: one {@code id INT64} predicate column and 16 {@code DOUBLE} output columns.
 * The benchmark has three axes: {@code layout}, {@code lateMaterialize}, and {@code selectivity}.
 *
 * <p><strong>Layout and the interaction with page pruning.</strong> With {@code SHUFFLED} ids every page's column-index
 * bounds span the entire id domain; column-index page pruning cannot skip any page and every page is decoded. Late
 * materialization is then the only lever: it decodes the predicate column first, evaluates the filter per row, and
 * decodes the 16 output columns only for rows that pass. At low selectivity (1% or one row) this skips output-column
 * decode for nearly all rows and is significantly faster.
 *
 * <p>With {@code SORTED} ids the matching rows cluster into a small band of adjacent pages. Column-index pruning
 * already skips all non-matching pages before a single output byte is decoded, leaving late materialization very little
 * additional work to avoid. The two arms approach parity in this case, mirroring the layout dependence shown by
 * {@code PagePruningBenchmark}.
 *
 * <p><strong>Selectivity.</strong> The win is largest at low selectivity where the ratio of decoded-but-rejected rows
 * is highest. {@code HALF} (50%) is omitted because late materialization saves little there and the measurement adds
 * noise without insight.
 *
 * <p>Both arms always return identical rows.
 *
 * <p>Run it with the shaded jar built under the {@code benchmarks} profile:
 *
 * <pre>{@code
 * ./mvnw -Pbenchmarks -pl :parquetry-benchmarks -am package
 * java --enable-preview --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow \
 *   -jar internal/parquetry-benchmarks/target/benchmarks.jar LateMaterializationBenchmark
 * }</pre>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
public class LateMaterializationBenchmark {

    private static final int ROWS = 200_000;
    private static final int VALUE_COLUMNS = 16;
    private static final int PAGE_VALUES = 4_096;
    private static final int SMOKE_ROWS = 4_000;
    private static final int SMOKE_VALUE_COLUMNS = 4;
    private static final int SMOKE_PAGE_VALUES = 512;
    private static final long SHUFFLE_SEED = 42L;

    /**
     * Disk order of the {@code id} column: clustered (page pruning effective) or scattered (page pruning cannot help).
     */
    public enum Layout {
        SORTED,
        SHUFFLED
    }

    /**
     * Fraction of rows the predicate matches. Values are defined as id-range bands over the actual row count, which
     * keeps them meaningful at both the full and the smoke fixture sizes without floating-point truncation surprises.
     */
    public enum Selectivity {
        /** Exactly one matching row (point lookup). */
        POINT {
            @Override
            Predicate predicate(long rows) {
                long target = rows / 2L;
                return Pred.col(SyntheticParquet.ID).eq(target);
            }
        },
        /** Approximately 1 percent of rows. */
        P1 {
            @Override
            Predicate predicate(long rows) {
                long band = Math.max(1L, rows / 100L);
                long lo = (rows - band) / 2L;
                long hi = lo + band - 1L;
                return Pred.col(SyntheticParquet.ID)
                        .gtEq(lo)
                        .and(Pred.col(SyntheticParquet.ID).ltEq(hi));
            }
        },
        /** Approximately 10 percent of rows. */
        P10 {
            @Override
            Predicate predicate(long rows) {
                long band = Math.max(1L, rows / 10L);
                long lo = (rows - band) / 2L;
                long hi = lo + band - 1L;
                return Pred.col(SyntheticParquet.ID)
                        .gtEq(lo)
                        .and(Pred.col(SyntheticParquet.ID).ltEq(hi));
            }
        };

        abstract Predicate predicate(long rows);
    }

    @Param({"SORTED", "SHUFFLED"})
    private Layout layout;

    @Param({"POINT", "P1", "P10"})
    private Selectivity selectivity;

    @Param({"true", "false"})
    private boolean lateMaterialize;

    /** Shrinks the fixture to a tiny size for the CI sanity run; default off leaves real measurements full-size. */
    @Param({"false"})
    private boolean smoke;

    private int rowCount;
    private int valueColumns;
    private int pageValues;
    private Path workDir;
    private SyntheticParquet.OpenDataset open;
    private Predicate predicate;
    private Projection projection;
    private ReadOptions options;

    @Setup(Level.Trial)
    public void setUp() throws IOException {
        rowCount = smoke ? SMOKE_ROWS : ROWS;
        valueColumns = smoke ? SMOKE_VALUE_COLUMNS : VALUE_COLUMNS;
        pageValues = smoke ? SMOKE_PAGE_VALUES : PAGE_VALUES;
        workDir = Files.createTempDirectory("parquetry-bench-late-mat-");
        Path file = workDir.resolve("late-mat.parquet");
        long[] ids = idsForLayout();
        SyntheticParquet.writeWideFile(file, writeOptions(), ids, valueColumns);
        open = SyntheticParquet.open(file);
        predicate = selectivity.predicate(rowCount);
        // Project only the output columns - id is the predicate column and is excluded from the output
        // to let output-column decode dominate, which measures late-mat savings cleanly.
        projection = SyntheticParquet.wideValueProjection(valueColumns);
        options = ReadOptions.builder().useLateMaterialization(lateMaterialize).build();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        open.close();
        SyntheticParquet.deleteRecursively(workDir);
    }

    /**
     * Reads matching rows and sums the first output column ({@code v0}) across all results. The sum is returned to
     * prevent the JIT from treating the read as dead code.
     */
    @Benchmark
    public double filteredRead() {
        try (Stream<ParquetRecord> rows = open.reader().read(predicate, projection, options)) {
            return rows.mapToDouble(rec -> rec.getDouble(SyntheticParquet.valueColumn(0)))
                    .sum();
        }
    }

    private long[] idsForLayout() {
        return layout == Layout.SORTED
                ? SyntheticParquet.sortedIds(rowCount)
                : SyntheticParquet.shuffledIds(rowCount, SHUFFLE_SEED);
    }

    private WriteOptions writeOptions() {
        WriteOptions.Builder builder = WriteOptions.builder()
                .tempDir(workDir)
                .rowGroupSize(WriteOptions.RowGroupSize.rows(rowCount + 1L))
                .pageValueLimit(pageValues)
                .encodingPolicy("id", WriteOptions.EncodingPolicy.FORCE_PLAIN);
        for (int i = 0; i < valueColumns; i++) {
            builder.encodingPolicy("v" + i, WriteOptions.EncodingPolicy.FORCE_PLAIN);
        }
        return builder.build();
    }
}
