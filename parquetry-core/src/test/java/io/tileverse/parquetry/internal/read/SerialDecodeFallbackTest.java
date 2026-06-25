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
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ParquetRuntime;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.record.ParquetRecord;

/**
 * Proves that {@code maxDecodeAheadPerRead=0} routes all decodes through the inline (serial) path and never touches the
 * {@link DecodeExecutor} slot semaphore.
 *
 * <p>If any slot were acquired, {@link DecodeExecutor#availableSlots()} would drop below the pool size during or after
 * the read. The assertion at the end would then catch the leak.
 */
class SerialDecodeFallbackTest {

    @Test
    void maxDecodeAheadZeroDecodesInlineAndTouchesNoDecodePool(@TempDir Path tmp) throws Exception {
        int rows = 4_000;
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, rows);
        DecodeExecutor decodePool = DecodeExecutor.ofParallelism(4);
        SegmentPool pool = SegmentPool.create();
        long count;
        try (ByteRangeSource source = TestParquetFiles.openRangeReader(file)) {
            ParquetRuntime runtime = ParquetRuntime.builder()
                    .segmentPool(pool)
                    .decodeExecutor(decodePool)
                    .maxDecodeAhead(0)
                    .build();
            ParquetFileReader dataset = ParquetFileReader.open(source, runtime, Optional.empty());
            try (Stream<ParquetRecord> stream =
                    dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                count = stream.count();
            }
        }
        assertThat(count).isEqualTo(rows);
        assertThat(decodePool.availableSlots())
                .as("maxDecodeAhead=0 must decode inline and never acquire a decode slot")
                .isEqualTo(4);
        assertThat(pool.stats().outstandingBorrows()).isZero();
        decodePool.shutdownNow();
    }
}
