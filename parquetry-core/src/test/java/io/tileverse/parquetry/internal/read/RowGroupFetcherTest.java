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
package io.tileverse.parquetry.internal.read;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.tileverse.parquetry.data.ParquetReader;
import io.tileverse.parquetry.data.ParquetRuntime;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testsupport.CorpusFixtures;

/**
 * Read-through behavioral test for the mandatory row-group fetch routing through the {@link FetchBufferAllocator}
 * valve. Constructing a {@link RowGroupFetcher} in isolation needs a footer-derived {@code RowGroupChunks}, a
 * {@code FetchPlan}, and an {@code IndexSectionLoader}, which only the read pipeline assembles. A read against a real
 * file with a tiny {@link FetchBudget} exercises the exact wiring this test guards: the prefetcher's inline path calls
 * {@code RowGroupFetcher.fetch}, which now allocates each coalesced range through the valve. The test pins a
 * tiny-RAM/ample-disk runtime and asserts both that the mandatory fetch spilled to a disk mapping and that the rows
 * read are byte-identical to a default RAM runtime.
 */
class RowGroupFetcherTest {

    private static final Path FILE = CorpusFixtures.parquetTestingData().resolve("alltypes_plain.parquet");

    @Test
    @Timeout(30)
    void mandatoryFetchSpillsToDiskWhenRamIsExhaustedAndReadsCorrectly() {
        long ampleDisk = 256L << 20;
        ParquetRuntime ramRuntime = ParquetRuntime.builder().build();
        DiskBudget spillDisk = DiskBudget.ofBytes(ampleDisk);
        ParquetRuntime spillingRuntime = ParquetRuntime.builder()
                .fetchBudget(FetchBudget.ofBytes(1)) // below any coalesced range, forcing the mandatory fetch to spill
                .diskBudget(spillDisk)
                .build();

        List<Map<ColumnPath, Object>> baseline = readAllRows(ramRuntime, spillDisk, false);
        List<Map<ColumnPath, Object>> spilled = readAllRows(spillingRuntime, spillDisk, true);

        assertThat(spilled).as("rows read via the spilled mandatory fetch").isEqualTo(baseline);
    }

    /**
     * Reads every row into a list of column-to-value maps. When {@code expectSpill} is set, the disk budget is sampled
     * after the first row has been pulled (the first row group's mandatory fetch has run) and is asserted to have
     * dipped below capacity, proving the fetch mapped a spill file rather than allocating RAM.
     */
    private static List<Map<ColumnPath, Object>> readAllRows(
            ParquetRuntime runtime, DiskBudget diskBudget, boolean expectSpill) {
        long diskCapacity = diskBudget.capacity();
        try (ByteRangeSource source = ByteRangeSource.ofFile(FILE)) {
            ParquetReader dataset = ParquetReader.open(source, runtime, Optional.empty());
            List<ColumnPath> leaves = dataset.schema().leafColumns();
            try (Stream<ParquetRecord> rows =
                    dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                return collectRows(rows, leaves, diskBudget, diskCapacity, expectSpill);
            }
        }
    }

    private static List<Map<ColumnPath, Object>> collectRows(
            Stream<ParquetRecord> rows,
            List<ColumnPath> leaves,
            DiskBudget diskBudget,
            long diskCapacity,
            boolean expectSpill) {
        List<Map<ColumnPath, Object>> collected = new ArrayList<>();
        boolean sampledFirstRow = false;
        for (ParquetRecord row : (Iterable<ParquetRecord>) rows::iterator) {
            collected.add(snapshot(row, leaves));
            if (expectSpill && !sampledFirstRow) {
                assertThat(diskBudget.available())
                        .as("mandatory fetch mapped a spill file")
                        .isLessThan(diskCapacity);
                sampledFirstRow = true;
            }
        }
        return collected;
    }

    /** Materializes every leaf value for one row, copying binary leaves to bytes so equality compares by content. */
    private static Map<ColumnPath, Object> snapshot(ParquetRecord row, List<ColumnPath> leaves) {
        Map<ColumnPath, Object> values = new LinkedHashMap<>();
        for (ColumnPath leaf : leaves) {
            values.put(leaf, valueOf(row, leaf));
        }
        return values;
    }

    private static Object valueOf(ParquetRecord row, ColumnPath leaf) {
        if (row.isNull(leaf)) {
            return null;
        }
        Object raw = row.get(leaf);
        if (raw instanceof MemorySegment segment) {
            return Arrays.toString(segment.toArray(ValueLayout.JAVA_BYTE));
        }
        return raw;
    }
}
