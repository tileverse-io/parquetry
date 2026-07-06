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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.SequencedSet;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Query;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testsupport.CorpusFixtures;

/**
 * Exercises the core reader's produce path directly: a {@link Projection.Of} with a renamed physical column and an
 * injected constant flows through {@code read(predicate, projection, options)}, which decodes the physical column and
 * shapes the produce set in the reader (not at the catalog layer).
 */
class ParquetSourceProduceSetTest {

    private static final Path FILE = CorpusFixtures.parquetTestingData().resolve("alltypes_plain.parquet");

    @Test
    void coreReaderProducesRenamedPhysicalAndConstant() throws Exception {
        try (ByteRangeSource byteSource = ByteRangeSource.ofFile(FILE)) {
            ParquetSource source = ParquetSource.open(byteSource);
            ColumnPath firstLeaf = source.schema().leafColumns().get(0);
            ColumnPath renamed = ColumnPath.of("renamed_first");
            ColumnPath label = ColumnPath.of("label");
            SequencedSet<Projection.Column> columns = new LinkedHashSet<>(List.of(
                    new Projection.Column.Physical(renamed, firstLeaf),
                    new Projection.Column.Constant(label, new Value.StringVal("x"))));
            Projection projection = Projection.of(columns);

            try (Stream<ParquetRecord> rows = source.read(Predicate.ALWAYS_TRUE, projection, ReadOptions.DEFAULTS)) {
                List<ParquetRecord> materialized =
                        rows.map(ParquetRecord::detach).toList();
                assertThat(materialized).isNotEmpty();
                assertThat(materialized.get(0).schema().leafColumns()).containsExactly(renamed, label);
                assertThat(materialized)
                        .allSatisfy(row -> assertThat(row.getString(label)).isEqualTo("x"));
            }
        }
    }

    @Test
    void outputColumnsSelectAndReorderTheProducedColumns() throws Exception {
        try (ByteRangeSource byteSource = ByteRangeSource.ofFile(FILE)) {
            ParquetSource source = ParquetSource.open(byteSource);
            ColumnPath first = source.schema().leafColumns().get(0);
            ColumnPath second = source.schema().leafColumns().get(1);
            Query query = Query.builder(Predicate.ALWAYS_TRUE, Projection.ofPhysical(List.of(first, second)))
                    .outputColumns(List.of(second, first))
                    .build();

            try (Stream<ParquetRecord> rows = source.read(query, ReadOptions.DEFAULTS)) {
                ParquetRecord row = rows.map(ParquetRecord::detach).findFirst().orElseThrow();
                assertThat(row.schema().leafColumns()).containsExactly(second, first);
            }
        }
    }
}
