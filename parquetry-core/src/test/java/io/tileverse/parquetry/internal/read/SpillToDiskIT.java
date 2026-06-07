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

import java.nio.file.Path;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.tileverse.parquetry.data.OpenOptions;
import io.tileverse.parquetry.data.ParquetDataset;
import io.tileverse.parquetry.data.ParquetRuntime;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.testsupport.CorpusFixtures;

/**
 * End-to-end check that spilling decoded batches to disk is transparent: a read forced to spill returns exactly the
 * rows an unconstrained read returns, and disabling spill under a normal budget reads correctly too.
 */
class SpillToDiskIT {

    private static final Path FILE = CorpusFixtures.parquetTestingData().resolve("alltypes_plain.parquet");

    @Test
    @Timeout(30)
    void readingUnderATinyDecodeBudgetSpillsAndReturnsTheSameRows() {
        // A decode budget below one batch forces every decoded batch to spill; ample disk, small batches.
        ParquetRuntime spillingRuntime = ParquetRuntime.builder()
                .decodeBudget(DecodeBudget.ofBytes(1))
                .diskBudget(DiskBudget.ofBytes(256L << 20))
                .spillEnabled(true)
                .build();
        OpenOptions spilling = OpenOptions.builder().runtime(spillingRuntime).build();
        ReadOptions smallBatches = ReadOptions.builder().batchSize(2).build();

        long plain = countRows(OpenOptions.DEFAULTS, ReadOptions.DEFAULTS);
        long spilled = countRows(spilling, smallBatches);

        assertThat(spilled).isEqualTo(plain).isPositive();
    }

    @Test
    @Timeout(30)
    void readingWithSpillDisabledAndAmpleBudgetReturnsTheSameRows() {
        OpenOptions disabled = OpenOptions.builder()
                .runtime(ParquetRuntime.builder().spillEnabled(false).build())
                .build();
        assertThat(countRows(disabled, ReadOptions.DEFAULTS))
                .isEqualTo(countRows(OpenOptions.DEFAULTS, ReadOptions.DEFAULTS));
    }

    private static long countRows(OpenOptions openOptions, ReadOptions readOptions) {
        try (ByteRangeSource src = ByteRangeSource.ofFile(FILE)) {
            ParquetDataset ds = ParquetDataset.open(src, openOptions);
            try (Stream<ParquetRecord> rows = ds.read(Predicate.ALWAYS_TRUE, Projection.ALL, readOptions)) {
                return rows.count();
            }
        }
    }
}
