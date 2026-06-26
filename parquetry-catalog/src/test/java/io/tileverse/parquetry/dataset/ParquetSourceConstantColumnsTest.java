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
package io.tileverse.parquetry.dataset;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.OutputColumn;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Query;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testsupport.CorpusFixtures;

class ParquetSourceConstantColumnsTest {

    private static final Path FILE = CorpusFixtures.parquetTestingData().resolve("alltypes_plain.parquet");

    @Test
    void appendsConstantColumnToEveryRow() throws Exception {
        try (ByteRangeSource byteSource = ByteRangeSource.ofFile(FILE)) {
            ParquetSource source = ParquetSource.open(byteSource);
            ColumnPath yearPart = ColumnPath.of("year_part");
            List<OutputColumn> output = new ArrayList<>();
            for (ColumnPath leaf : source.schema().leafColumns()) {
                output.add(new OutputColumn.Physical(leaf, leaf));
            }
            output.add(new OutputColumn.Constant(yearPart, new Value.IntVal(2024)));
            Query query = Query.builder(Predicate.ALWAYS_TRUE, Projection.ALL)
                    .output(output)
                    .build();
            try (Stream<ParquetRecord> rows = source.read(query, ReadOptions.DEFAULTS)) {
                List<ParquetRecord> materialized =
                        rows.map(ParquetRecord::detach).toList();
                assertThat(materialized).isNotEmpty();
                assertThat(materialized)
                        .allSatisfy(row -> assertThat(row.getInt(ColumnPath.of("year_part")))
                                .isEqualTo(2024));
            }
        }
    }
}
