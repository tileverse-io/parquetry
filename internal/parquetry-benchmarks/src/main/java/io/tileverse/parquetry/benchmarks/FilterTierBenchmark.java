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
 * Measures how far each metadata filter tier reduces a selective point lookup.
 *
 * <p>The fixture is a sorted {@code id INT64 + value DOUBLE} table of ten row groups, written with small pages and a
 * bloom filter on {@code id}. An equality predicate on a single {@code id} in the middle row group matches exactly one
 * row, which gives every metadata tier something to prune:
 *
 * <ul>
 *   <li>STATS drops the nine row groups whose min/max bounds exclude the value.
 *   <li>BLOOM drops the row groups whose bloom filter does not contain the value (equality only).
 *   <li>COLUMN_INDEX drops, within the surviving row group, the pages whose bounds exclude the value.
 * </ul>
 *
 * <p>The {@code tiers} parameter selects which metadata tiers run; the record-level tier stays on throughout. Every
 * configuration therefore returns the same single row, and the only difference is how much IO and decode the metadata
 * pruning avoided. {@code RECORD_ONLY} is the no-pushdown baseline (decode everything, filter per row); {@code ALL} is
 * the full pipeline. The dictionary tier is not exercised here ({@code id} is high cardinality and written as PLAIN);
 * it needs a low-cardinality column and belongs in a separate benchmark.
 *
 * <pre>{@code
 * ./mvnw -Pbenchmarks -pl :parquetry-benchmarks -am package
 * java --enable-preview --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow \
 *   -jar internal/parquetry-benchmarks/target/benchmarks.jar FilterTierBenchmark
 * }</pre>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
public class FilterTierBenchmark {

    private static final long TARGET_FRACTION_NUMERATOR = 555_555L;
    private static final long TARGET_FRACTION_DENOMINATOR = 1_000_000L;

    /**
     * Which metadata tiers run for a configuration. The record-level tier is always on; each value returns the same row
     * and differs only in pruning effort.
     */
    public enum Tiers {
        RECORD_ONLY(false, false, false),
        STATS(true, false, false),
        COLUMN_INDEX(false, true, false),
        BLOOM(false, false, true),
        ALL(true, true, true);

        private final boolean stats;
        private final boolean columnIndex;
        private final boolean bloom;

        Tiers(boolean stats, boolean columnIndex, boolean bloom) {
            this.stats = stats;
            this.columnIndex = columnIndex;
            this.bloom = bloom;
        }

        ReadOptions toReadOptions() {
            return ReadOptions.builder()
                    .useStatsFilter(stats)
                    .useDictionaryFilter(false)
                    .useColumnIndexFilter(columnIndex)
                    .useBloomFilter(bloom)
                    .useRecordLevelFilter(true)
                    .build();
        }
    }

    @Param({"RECORD_ONLY", "STATS", "COLUMN_INDEX", "BLOOM", "ALL"})
    private Tiers tiers;

    /**
     * Shrinks the fixture to a few thousand rows across a handful of row groups for the CI sanity run that only checks
     * the benchmark still runs. The default keeps the full measurement workload; it is not a measurement mode.
     */
    @Param({"false"})
    private boolean smoke;

    private int rowsPerGroup;
    private int pageValues;

    private Path workDir;
    private SyntheticParquet.OpenDataset open;
    private Predicate predicate;
    private Projection projection;
    private ReadOptions options;

    @Setup(Level.Trial)
    public void setUp() throws IOException {
        int rows = smoke ? 4_000 : 1_000_000;
        rowsPerGroup = smoke ? 1_000 : 100_000;
        pageValues = smoke ? 256 : 2_048;
        // The target lands in a middle row group, on a page interior to it. Every metadata tier then has something to
        // prune at either size.
        long targetId = rows * TARGET_FRACTION_NUMERATOR / TARGET_FRACTION_DENOMINATOR;

        workDir = Files.createTempDirectory("parquetry-bench-filter-tier-");
        Path file = workDir.resolve("filter-tier.parquet");
        SyntheticParquet.writeIdValueFile(file, writeOptions(), SyntheticParquet.sortedIds(rows));
        open = SyntheticParquet.open(file);
        predicate = Pred.col(SyntheticParquet.ID).eq(targetId);
        projection = Projection.ALL;
        options = tiers.toReadOptions();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        open.close();
        SyntheticParquet.deleteRecursively(workDir);
    }

    @Benchmark
    public double pointLookup() {
        try (Stream<ParquetRecord> rows = open.dataset().read(predicate, projection, options)) {
            return rows.mapToDouble(rec -> rec.getDouble(SyntheticParquet.VALUE))
                    .sum();
        }
    }

    private WriteOptions writeOptions() {
        return WriteOptions.builder()
                .tempDir(workDir)
                .rowGroupSize(WriteOptions.RowGroupSize.rows(rowsPerGroup))
                .pageValueLimit(pageValues)
                .encodingPolicy("id", WriteOptions.EncodingPolicy.FORCE_PLAIN)
                .encodingPolicy("value", WriteOptions.EncodingPolicy.FORCE_PLAIN)
                .bloomFilter("id", new WriteOptions.BloomFilterConfig(0.01, rowsPerGroup))
                .build();
    }
}
