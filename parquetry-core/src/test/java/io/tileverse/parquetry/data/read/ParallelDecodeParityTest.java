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
package io.tileverse.parquetry.data.read;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ParquetDataset;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testsupport.CountingSegmentPool;

/**
 * Proves that the parallel decode path returns identical records in identical (strict file) order as the serial path.
 *
 * <p>Reads the same multi-row-group file twice: once with serial inline decode ({@code maxDecodeAheadPerRead=0}) and
 * once with a dedicated {@link DecodeExecutor} and a decode-ahead window of 3. The rendered row sequences must be
 * element-for-element equal.
 */
class ParallelDecodeParityTest {

    private static final ColumnPath YEAR = ColumnPath.of("year");
    private static final ColumnPath COUNTRY = ColumnPath.of("country");
    private static final ColumnPath VALUE = ColumnPath.of("value");

    @Test
    void parallelDecodeMatchesSerialRecordsAndOrder(@TempDir Path tmp) throws Exception {
        int rows = 4_000;
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, rows);

        List<String> serial = read(file, /*decodeAhead*/ 0, DecodeExecutor.shared());

        DecodeExecutor pool = DecodeExecutor.ofParallelism(4);
        List<String> parallel;
        try {
            parallel = read(file, /*decodeAhead*/ 3, pool);
        } finally {
            pool.shutdownNow();
        }

        assertThat(serial).hasSize(rows);
        assertThat(parallel)
                .as("parallel decode yields the same rows in the same file order as serial decode")
                .containsExactlyElementsOf(serial);
    }

    private static List<String> read(Path file, int decodeAhead, DecodeExecutor executor) throws Exception {
        CountingSegmentPool pool = new CountingSegmentPool();
        List<String> rendered = new ArrayList<>();
        try (ByteRangeSource source = TestParquetFiles.openRangeReader(file)) {
            ParquetDataset dataset = ParquetDataset.open(source);
            ReadOptions options = ReadOptions.builder()
                    .segmentPool(pool)
                    .decodeExecutor(executor)
                    .maxDecodeAheadPerRead(decodeAhead)
                    .build();
            try (Stream<ParquetRecord> stream = dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, options)) {
                stream.forEach(record -> rendered.add(renderKey(record)));
            }
        }
        assertThat(pool.outstanding()).isZero();
        return rendered;
    }

    private static String renderKey(ParquetRecord record) {
        return record.getInt(YEAR) + "|" + record.getString(COUNTRY) + "|" + record.getDouble(VALUE);
    }
}
