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
package io.tileverse.parquetry.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testsupport.CorpusFixtures;

/**
 * A V2 data page of null cells alone holds no value bytes. In a compressed column the page stores zero bytes for its
 * values, a stream that no codec decodes: the reader takes them for the empty values they are.
 *
 * <p>The corpus file holds one row of a Snappy-compressed FLOAT column, a null.
 */
class DataPageWithoutValueBytesTest {

    private static final Path FILE =
            CorpusFixtures.parquetTestingData().resolve("datapage_v2_empty_datapage.snappy.parquet");
    private static final ColumnPath VALUE = ColumnPath.of("value");

    // The page declares one row of nulls and stores no value bytes.
    @Test
    void readsItsRowAsNull() {
        try (ByteRangeSource source = ByteRangeSource.ofFile(FILE)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            List<ParquetRecord> rows = readRows(reader);

            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).isNull(VALUE)).isTrue();
        }
    }

    @Test
    void batchesAndCountHoldItsRow() {
        try (ByteRangeSource source = ByteRangeSource.ofFile(FILE)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            assertThat(rowsInBatches(reader)).isEqualTo(1L);
            assertThat(reader.count(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS))
                    .isEqualTo(1L);
        }
    }

    private static List<ParquetRecord> readRows(ParquetFileReader reader) {
        try (Stream<ParquetRecord> rows = reader.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
            return rows.toList();
        }
    }

    private static long rowsInBatches(ParquetFileReader reader) {
        try (Stream<ParquetRecordBatch> batches =
                reader.readBatches(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
            return batches.mapToLong(ParquetRecordBatch::rowCount).sum();
        }
    }
}
