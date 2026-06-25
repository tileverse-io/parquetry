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
package io.tileverse.parquetry.cli.arrow;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.cli.support.Fixtures;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.ParquetSource;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.tileverse.ByteRangeSources;

class RecordBatchPackerTest {

    @Test
    void packsAllRowsIntoOneBatch(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("cities.parquet");
        Fixtures.writeCities(file);
        try (Storage storage = StorageFactory.open(dir.toUri());
                RangeReader reader = storage.openRangeReader(file.getFileName().toString())) {
            ParquetSource source = ParquetSource.open(ByteRangeSources.from(reader));
            ParquetSchema schema = source.schema();
            List<ParquetRecordBatch> batches = packAll(source, schema, 1024);
            try {
                assertThat(batches).hasSize(1);
                ParquetRecordBatch batch = batches.get(0);
                assertThat(batch.rowCount()).isEqualTo(4);
                assertThat(batch.columns().keySet()).containsExactlyInAnyOrderElementsOf(schema.leafColumns());
                assertThat(batch.materialize(0).getString(Fixtures.NAME)).isEqualTo("Rosario");
                assertThat(batch.materialize(0).getLong(Fixtures.POP)).isEqualTo(1_300_000L);
                assertThat(batch.materialize(3).isNull(Fixtures.NAME)).isTrue();
            } finally {
                closeAll(batches);
            }
        }
    }

    @Test
    void chunksAtTargetRowCount(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("cities.parquet");
        Fixtures.writeCities(file);
        try (Storage storage = StorageFactory.open(dir.toUri());
                RangeReader reader = storage.openRangeReader(file.getFileName().toString())) {
            ParquetSource source = ParquetSource.open(ByteRangeSources.from(reader));
            ParquetSchema schema = source.schema();
            List<ParquetRecordBatch> batches = packAll(source, schema, 2);
            try {
                assertThat(batches).hasSize(2);
                assertThat(batches.get(0).rowCount()).isEqualTo(2);
                assertThat(batches.get(1).rowCount()).isEqualTo(2);
            } finally {
                closeAll(batches);
            }
        }
    }

    private static List<ParquetRecordBatch> packAll(ParquetSource source, ParquetSchema schema, int targetRows) {
        try (Stream<ParquetRecord> rows = source.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
            return RecordBatchPacker.pack(rows, schema, targetRows).toList();
        }
    }

    private static void closeAll(List<ParquetRecordBatch> batches) {
        for (ParquetRecordBatch batch : batches) {
            batch.close();
        }
    }
}
