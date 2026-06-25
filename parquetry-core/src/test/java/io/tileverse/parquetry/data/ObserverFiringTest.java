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
package io.tileverse.parquetry.data;

import static io.tileverse.parquetry.filter.Pred.col;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.observe.QueryObserver;
import io.tileverse.parquetry.observe.QueryStarted;
import io.tileverse.parquetry.observe.QueryStats;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Verifies that {@code onQueryStarted} and {@code onRowGroupPlanned} fire once per query from the shared filter
 * pipeline, on every read entry point: {@code explain}, {@code count}, {@code read}, and {@code readBatches}.
 */
class ObserverFiringTest {

    private static final Predicate PREDICATE = col("id").gtEq(0);

    @Test
    void explainFiresQueryStartedAndPlannedDecisions(@TempDir Path tmp) throws Exception {
        Path file = corpusFile(tmp);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            RecordingObserver observer = new RecordingObserver();
            ReadOptions options = optionsWith(observer);

            reader.explain(PREDICATE, Projection.ALL, options);

            assertFired(observer);
        }
    }

    @Test
    void countFiresQueryStartedAndPlannedDecisions(@TempDir Path tmp) throws Exception {
        Path file = corpusFile(tmp);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            RecordingObserver observer = new RecordingObserver();
            ReadOptions options = optionsWith(observer);

            reader.count(PREDICATE, options);

            assertFired(observer);
        }
    }

    @Test
    void readFiresQueryStartedAndPlannedDecisions(@TempDir Path tmp) throws Exception {
        Path file = corpusFile(tmp);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            RecordingObserver observer = new RecordingObserver();
            ReadOptions options = optionsWith(observer);

            try (Stream<?> rows = reader.read(PREDICATE, Projection.ALL, options)) {
                rows.forEach(row -> {});
            }

            assertFired(observer);
        }
    }

    @Test
    void readBatchesFiresQueryStartedAndPlannedDecisions(@TempDir Path tmp) throws Exception {
        Path file = corpusFile(tmp);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            RecordingObserver observer = new RecordingObserver();
            ReadOptions options = optionsWith(observer);

            try (Stream<?> batches = reader.readBatches(PREDICATE, Projection.ALL, options)) {
                batches.forEach(batch -> {});
            }

            assertFired(observer);
        }
    }

    @Test
    void countFiresQueryFinishedOnce(@TempDir Path tmp) throws Exception {
        Path file = corpusFile(tmp);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            RecordingObserver observer = new RecordingObserver();
            ReadOptions options = optionsWith(observer);

            reader.count(PREDICATE, options);

            assertThat(observer.finishedCount.get())
                    .as("onQueryFinished fires exactly once on the eager count path")
                    .isEqualTo(1);
            assertThat(observer.lastStats).isNotNull();
        }
    }

    @Test
    void readFiresQueryFinishedOnStreamClose(@TempDir Path tmp) throws Exception {
        Path file = corpusFile(tmp);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            RecordingObserver observer = new RecordingObserver();
            ReadOptions options = optionsWith(observer);

            Stream<?> rows = reader.read(PREDICATE, Projection.ALL, options);
            rows.forEach(row -> {});
            assertThat(observer.finishedCount.get())
                    .as("onQueryFinished waits for the stream to close")
                    .isZero();

            rows.close();

            assertThat(observer.finishedCount.get())
                    .as("onQueryFinished fires once the consumer closes the stream")
                    .isEqualTo(1);
            assertThat(observer.lastStats).isNotNull();
        }
    }

    @Test
    void readBatchesFiresQueryFinishedOnStreamClose(@TempDir Path tmp) throws Exception {
        Path file = corpusFile(tmp);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            RecordingObserver observer = new RecordingObserver();
            ReadOptions options = optionsWith(observer);

            Stream<?> batches = reader.readBatches(PREDICATE, Projection.ALL, options);
            batches.forEach(batch -> {});
            assertThat(observer.finishedCount.get())
                    .as("onQueryFinished waits for the stream to close")
                    .isZero();

            batches.close();

            assertThat(observer.finishedCount.get())
                    .as("onQueryFinished fires once the consumer closes the stream")
                    .isEqualTo(1);
            assertThat(observer.lastStats).isNotNull();
        }
    }

    @Test
    void noObserverFiresNothing(@TempDir Path tmp) throws Exception {
        Path file = corpusFile(tmp);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            long count = reader.count(PREDICATE, ReadOptions.DEFAULTS);
            long[] streamed = {0L};
            try (Stream<?> rows = reader.read(PREDICATE, Projection.ALL, ReadOptions.DEFAULTS)) {
                rows.forEach(row -> streamed[0]++);
            }
            try (Stream<?> batches = reader.readBatches(PREDICATE, Projection.ALL, ReadOptions.DEFAULTS)) {
                batches.forEach(batch -> {});
            }

            assertThat(streamed[0])
                    .as("the default no-observer read path returns the same rows count() reports")
                    .isEqualTo(count);
        }
    }

    private static void assertFired(RecordingObserver observer) {
        assertThat(observer.queryStartedCount.get())
                .as("onQueryStarted fires exactly once per query")
                .isEqualTo(1);
        assertThat(observer.plannedDecisions)
                .as("onRowGroupPlanned replays every tier decision of every row group")
                .isNotEmpty();
    }

    private static Path corpusFile(Path tmp) {
        return TestCorpus.extractFile("parquet-testing/data/alltypes_plain.parquet", tmp);
    }

    private static ReadOptions optionsWith(QueryObserver observer) {
        return ReadOptions.builder().queryObserver(observer).build();
    }

    private static final class RecordingObserver implements QueryObserver {

        private final AtomicInteger queryStartedCount = new AtomicInteger();
        private final AtomicInteger finishedCount = new AtomicInteger();
        private final List<PlannedDecision> plannedDecisions = new ArrayList<>();
        private volatile QueryStats lastStats;

        @Override
        public void onQueryStarted(QueryStarted event) {
            queryStartedCount.incrementAndGet();
        }

        @Override
        public void onRowGroupPlanned(int rowGroup, PruningDecision decision) {
            plannedDecisions.add(new PlannedDecision(rowGroup, decision));
        }

        @Override
        public void onQueryFinished(QueryStats stats) {
            finishedCount.incrementAndGet();
            this.lastStats = stats;
        }
    }

    private record PlannedDecision(int rowGroup, PruningDecision decision) {}
}
