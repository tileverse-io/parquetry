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
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * The materializer overload of the query read: a shaped query hands the materializer rows that already present the
 * output shape (here a constant column), and an identity query applies the offset/limit window to the materialized rows
 * exactly as the record overload does.
 */
class ParquetSourceMaterializerQueryTest {

    private static final ColumnPath YEAR = ColumnPath.of("year");
    private static final ColumnPath VALUE = ColumnPath.of("value");
    private static final ColumnPath REGION = ColumnPath.of("region");

    @Test
    void shapedQueryHandsTheMaterializerRowsInTheOutputShape(@TempDir Path tmp) throws Exception {
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, 300);
        try (ByteRangeSource bytes = TestParquetFiles.openRangeReader(file)) {
            ParquetSource source = ParquetSource.open(bytes);
            SequencedSet<Projection.Column> columns = new LinkedHashSet<>();
            columns.add(new Projection.Column.Physical(YEAR, YEAR));
            columns.add(new Projection.Column.Constant(REGION, new Value.StringVal("emea")));
            Query query = Query.builder(Predicate.ALWAYS_TRUE, Projection.of(columns))
                    .outputColumns(List.of(YEAR, REGION))
                    .build();
            Materializer<String> keyed = (schema, row) -> row.getInt(YEAR) + "|" + row.getString(REGION);

            List<String> rows;
            try (Stream<String> stream = source.read(query, keyed, ReadOptions.DEFAULTS)) {
                rows = stream.toList();
            }

            assertThat(rows).hasSize(300).allSatisfy(row -> assertThat(row).endsWith("|emea"));
        }
    }

    @Test
    void identityQueryWindowsTheMaterializedRows(@TempDir Path tmp) throws Exception {
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, 300);
        try (ByteRangeSource bytes = TestParquetFiles.openRangeReader(file)) {
            ParquetSource source = ParquetSource.open(bytes);
            List<Double> reference;
            try (Stream<ParquetRecord> all = source.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                reference = all.map(row -> row.getDouble(VALUE)).toList();
            }
            Query windowed = Query.builder(Predicate.ALWAYS_TRUE, Projection.ALL)
                    .offset(100)
                    .limit(50)
                    .build();

            List<Double> window;
            try (Stream<Double> stream =
                    source.read(windowed, (schema, row) -> row.getDouble(VALUE), ReadOptions.DEFAULTS)) {
                window = stream.toList();
            }

            assertThat(window).isEqualTo(reference.subList(100, 150));
        }
    }
}
