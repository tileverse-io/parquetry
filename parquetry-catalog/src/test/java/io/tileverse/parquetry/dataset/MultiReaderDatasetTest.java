/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.RowGroupSummary;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.testsupport.CorpusFixtures;

class MultiReaderDatasetTest {

    private static final Path FILE = CorpusFixtures.parquetTestingData().resolve("alltypes_plain.parquet");

    @Test
    void multiReaderConcatenatesRowsAndAggregatesRowGroups() {
        try (ByteRangeSource a = ByteRangeSource.ofFile(FILE);
                ByteRangeSource b = ByteRangeSource.ofFile(FILE)) {

            ParquetFileReader ra = ParquetFileReader.open(a);
            ParquetFileReader rb = ParquetFileReader.open(b);
            long single =
                    ra.rowGroups().stream().mapToLong(RowGroupSummary::rowCount).sum();

            DefaultParquetSource ds = new DefaultParquetSource(List.of(ra, rb), 8);

            // same file twice means identical schemas; construction succeeds
            assertThat(ds.schema()).isEqualTo(ra.schema());

            long streamed;
            try (Stream<ParquetRecord> rows = ds.read()) {
                streamed = rows.count();
            }
            assertThat(streamed).isEqualTo(2 * single);

            // rowGroups are concatenated with sequential indices
            List<RowGroupSummary> groups = ds.rowGroups();
            assertThat(groups).hasSize(2 * ra.rowGroups().size());
            assertThat(groups)
                    .extracting(RowGroupSummary::index)
                    .containsExactlyElementsOf(java.util.stream.IntStream.range(0, groups.size())
                            .boxed()
                            .toList());
            assertThat(groups.stream().mapToLong(RowGroupSummary::rowCount).sum())
                    .isEqualTo(2 * single);
        }
    }
}
