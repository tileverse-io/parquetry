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

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.internal.read.DiskBudget;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.limits.ResourceLimits;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testsupport.CorpusFixtures;

/**
 * Acceptance test for the read path under a pinned tiny native budget: a full read whose every coalesced row-group
 * range exceeds the fetch budget must route its mandatory fetch through a disk mapping rather than fail, and return the
 * same values it would on an unconstrained RAM read.
 *
 * <p>The pinned runtime is built from a {@link ResourceLimits#fixed deterministic} resource fact: a memory of 10 bytes
 * derives a fetch budget of {@code max(1, 10 * 0.1) = 1} byte (below any real range, forcing every mandatory fetch to
 * spill), while an 8&nbsp;GiB disk derives a 4&nbsp;GiB disk budget (ample for the spill). The default runtime reads
 * the same file from RAM. Both reads are compared cell-by-cell across the full dataset.
 */
class MmapFetchSpillIT {

    private static final Path FILE = CorpusFixtures.parquetTestingData().resolve("alltypes_plain.parquet");

    private static final long PINNED_MEMORY_BYTES = 10L;
    private static final long AMPLE_DISK_BYTES = 8L << 30;
    private static final int PINNED_PROCESSORS = 1;

    @Test
    @Timeout(60)
    void pinnedTinyBudgetSpillsTheMandatoryFetchAndReadsIdenticalValues(@TempDir Path spillDir) {
        ParquetRuntime ramRuntime = ParquetRuntime.defaultRuntime();
        ParquetRuntime pinnedRuntime = ParquetRuntime.builder()
                .resourceLimits(
                        ResourceLimits.fixed(PINNED_MEMORY_BYTES, AMPLE_DISK_BYTES, PINNED_PROCESSORS, spillDir))
                .build();

        assertThat(pinnedRuntime.fetchBudget().capacity())
                .as("a 10-byte memory derives a 1-byte fetch budget, below any real coalesced range")
                .isEqualTo(1L);
        assertThat(pinnedRuntime.diskBudget().capacity())
                .as("an 8 GiB disk derives a 4 GiB disk budget, ample for the spill")
                .isEqualTo(AMPLE_DISK_BYTES / 2);

        List<Map<ColumnPath, Object>> baseline = readAllRows(ramRuntime, null);
        List<Map<ColumnPath, Object>> spilled = readAllRows(pinnedRuntime, pinnedRuntime.diskBudget());

        assertThat(spilled)
                .as("rows read through the spilled mandatory fetch match the RAM read cell-by-cell")
                .usingRecursiveComparison()
                .isEqualTo(baseline);
    }

    /**
     * Reads every row into a list of column-to-value maps. When {@code spillBudget} is non-null the disk budget is
     * sampled after the first row has been pulled (the first row group's mandatory fetch has run) and is asserted to
     * have dipped below capacity, proving the fetch mapped a spill file rather than allocating RAM.
     */
    private static List<Map<ColumnPath, Object>> readAllRows(ParquetRuntime runtime, DiskBudget spillBudget) {
        OpenOptions openOptions = OpenOptions.builder().runtime(runtime).build();
        try (ByteRangeSource source = ByteRangeSource.ofFile(FILE)) {
            ParquetDataset dataset = ParquetDataset.open(source, openOptions);
            List<ColumnPath> leaves = dataset.schema().leafColumns();
            try (Stream<ParquetRecord> rows =
                    dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                return collectRows(rows, leaves, spillBudget);
            }
        }
    }

    private static List<Map<ColumnPath, Object>> collectRows(
            Stream<ParquetRecord> rows, List<ColumnPath> leaves, DiskBudget spillBudget) {
        List<Map<ColumnPath, Object>> collected = new ArrayList<>();
        boolean sampledFirstRow = false;
        for (ParquetRecord row : (Iterable<ParquetRecord>) rows::iterator) {
            collected.add(snapshot(row, leaves));
            if (spillBudget != null && !sampledFirstRow) {
                assertThat(spillBudget.available())
                        .as("mandatory fetch mapped a spill file under the 1-byte fetch budget")
                        .isLessThan(spillBudget.capacity());
                sampledFirstRow = true;
            }
        }
        return collected;
    }

    /**
     * Materializes every leaf value for one row; binary leaves become their raw byte content for structural equality.
     */
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
            return segment.toArray(ValueLayout.JAVA_BYTE);
        }
        return raw;
    }
}
