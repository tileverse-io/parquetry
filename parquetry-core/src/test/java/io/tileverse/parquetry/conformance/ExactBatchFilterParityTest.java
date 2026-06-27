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
package io.tileverse.parquetry.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.internal.read.TestParquetFiles;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Proves the three read entry points agree on the exact surviving row set over a predicate battery: {@code read},
 * {@code readBatches}, and {@code count} must select identical rows in file order, not merely the same cardinality.
 */
class ExactBatchFilterParityTest {

    private static final ColumnPath YEAR = ColumnPath.of("year");
    private static final ColumnPath COUNTRY = ColumnPath.of("country");
    private static final ColumnPath VALUE = ColumnPath.of("value");

    static List<Predicate> battery() {
        return List.of(
                Predicate.ALWAYS_TRUE,
                Pred.col("year").gtEq(2020),
                Pred.col("year").gt(9_999),
                Pred.col("year").eq(2022),
                Pred.col("year").inInts(2020, 2024),
                Pred.col("value").lt(3_000.0),
                Pred.col("country").isNotNull(),
                Pred.col("country").eq("AR"),
                Pred.col("year").eq(2022).negate());
    }

    @ParameterizedTest
    @MethodSource("battery")
    void readEqualsReadBatchesEqualsCount(Predicate predicate, @TempDir Path tmp) throws Exception {
        assertReadEqualsReadBatchesEqualsCount(predicate, ReadOptions.DEFAULTS, tmp);
    }

    @ParameterizedTest
    @MethodSource("battery")
    void readEqualsReadBatchesEqualsCountWithoutLateMaterialization(Predicate predicate, @TempDir Path tmp)
            throws Exception {
        ReadOptions options =
                ReadOptions.builder().useLateMaterialization(false).build();
        assertReadEqualsReadBatchesEqualsCount(predicate, options, tmp);
    }

    private static void assertReadEqualsReadBatchesEqualsCount(Predicate predicate, ReadOptions options, Path tmp)
            throws Exception {
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, 4_000);
        try (ByteRangeSource source = TestParquetFiles.openRangeReader(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            List<String> viaRead = rowsFromRead(reader, predicate, options);
            List<String> viaBatches = rowsFromBatches(reader, predicate, options);
            long viaCount = reader.count(predicate, options);

            assertThat(viaBatches)
                    .as("readBatches must select the same rows in the same order as read")
                    .isEqualTo(viaRead);
            assertThat((long) viaBatches.size())
                    .as("count must equal the surviving row-set size")
                    .isEqualTo(viaCount);
        }
    }

    private static List<String> rowsFromRead(ParquetFileReader reader, Predicate predicate, ReadOptions options) {
        List<String> rows = new ArrayList<>();
        try (Stream<ParquetRecord> records = reader.read(predicate, Projection.ALL, options)) {
            records.forEach(rec -> rows.add(key(rec)));
        }
        return rows;
    }

    private static List<String> rowsFromBatches(ParquetFileReader reader, Predicate predicate, ReadOptions options) {
        List<String> rows = new ArrayList<>();
        try (Stream<ParquetRecordBatch> batches = reader.readBatches(predicate, Projection.ALL, options)) {
            batches.forEach(batch -> {
                for (int row = 0; row < batch.rowCount(); row++) {
                    rows.add(key(batch.materialize(row)));
                }
                batch.close();
            });
        }
        return rows;
    }

    private static String key(ParquetRecord rec) {
        StringBuilder builder = new StringBuilder();
        builder.append(rec.getInt(YEAR)).append('|');
        builder.append(rec.getString(COUNTRY)).append('|');
        builder.append(rec.getDouble(VALUE));
        return builder.toString();
    }
}
