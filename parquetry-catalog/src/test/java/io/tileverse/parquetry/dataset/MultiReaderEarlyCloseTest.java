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
package io.tileverse.parquetry.dataset;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.SequencedSet;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Query;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.internal.read.TestParquetFiles;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.runtime.FetchBudget;
import io.tileverse.parquetry.runtime.ParquetRuntime;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Proves that early-terminating a read over a multi-file dataset drains the segment pool across every reader. A
 * multi-file {@link ParquetSource} stitches the per-file reads with {@code flatMap}; consuming rows from a later file
 * and then closing the outer stream must close both the exhausted earlier inner streams and the in-flight current one,
 * leaving no borrowed buffers outstanding and the fetch budget fully restored.
 */
class MultiReaderEarlyCloseTest {

    @Test
    void earlyCloseAcrossReadersDrainsPoolAndBudget(@TempDir Path tmp) throws Exception {
        int rowsPerFile = 4_000;
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, rowsPerFile);
        SegmentPool pool = SegmentPool.create();
        FetchBudget budget = FetchBudget.ofMaxMemoryFraction(0.1);
        long capacityBefore = budget.available();

        try (ByteRangeSource first = TestParquetFiles.openRangeReader(file);
                ByteRangeSource second = TestParquetFiles.openRangeReader(file)) {

            OpenOptions openOptions = OpenOptions.builder()
                    .runtime(ParquetRuntime.builder()
                            .segmentPool(pool)
                            .fetchBudget(budget)
                            .prefetchDepth(4)
                            .build())
                    .build();
            ParquetSource source = ParquetSource.open(twoFileFileset(first, second), openOptions);

            try (Stream<ParquetRecord> stream =
                    source.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                Iterator<ParquetRecord> it = stream.iterator();
                // Exhaust the first reader and step one row into the second, then close without draining the rest.
                for (int consumed = 0; consumed <= rowsPerFile; consumed++) {
                    assertThat(it.hasNext()).isTrue();
                    it.next();
                }
            }
        }

        assertThat(pool.stats().outstandingBorrows())
                .as("buffers borrowed across both readers are returned on early close")
                .isZero();
        assertThat(budget.available())
                .as("reserved fetch budget is restored across both readers on early close")
                .isEqualTo(capacityBefore);
    }

    /**
     * The synthetic-column read path flattens each file's augmented batches into rows with
     * {@code readBatches(query).flatMap(ConstantColumnBatches::rows)}, where each rows substream attaches
     * {@code onClose(batch::close)}. A consumer that takes a few rows and then closes the stream early - while a later
     * batch is still in flight - must still close that in-flight batch. Driving {@link ParquetSource#read(Query,
     * ReadOptions)} with a constant column exercises that {@code flatMap} layer; the pool and budget returning to their
     * pre-read baselines prove the early close cascades to the open batch.
     */
    @Test
    void earlyCloseOfConstantColumnReadReleasesInFlightBatch(@TempDir Path tmp) throws Exception {
        int rowsPerFile = 4_000;
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, rowsPerFile);
        SegmentPool pool = SegmentPool.create();
        FetchBudget budget = FetchBudget.ofMaxMemoryFraction(0.1);
        long capacityBefore = budget.available();

        try (ByteRangeSource first = TestParquetFiles.openRangeReader(file);
                ByteRangeSource second = TestParquetFiles.openRangeReader(file)) {

            OpenOptions openOptions = OpenOptions.builder()
                    .runtime(ParquetRuntime.builder()
                            .segmentPool(pool)
                            .fetchBudget(budget)
                            .prefetchDepth(4)
                            .build())
                    .build();
            ParquetSource source = ParquetSource.open(twoFileFileset(first, second), openOptions);

            SequencedSet<Projection.Column> columns = new LinkedHashSet<>();
            for (ColumnPath leaf : source.schema().leafColumns()) {
                columns.add(new Projection.Column.Physical(leaf, leaf));
            }
            columns.add(new Projection.Column.Constant(ColumnPath.of("region"), new Value.StringVal("emea")));
            Query query = Query.of(Predicate.ALWAYS_TRUE, Projection.of(columns));

            try (Stream<ParquetRecord> stream = source.read(query, ReadOptions.DEFAULTS)) {
                Iterator<ParquetRecord> it = stream.iterator();
                // Drain the first reader and step one row into the second, then close without draining the rest.
                for (int consumed = 0; consumed <= rowsPerFile; consumed++) {
                    assertThat(it.hasNext()).isTrue();
                    it.next();
                }
            }
        }

        assertThat(pool.stats().outstandingBorrows())
                .as("the in-flight constant-column batch returns its borrowed buffers on early close")
                .isZero();
        assertThat(budget.available())
                .as("reserved fetch budget is restored across both readers on early close")
                .isEqualTo(capacityBefore);
    }

    private static FilesetReader twoFileFileset(ByteRangeSource first, ByteRangeSource second) {
        return TestFilesets.of(List.of(first, second));
    }
}
