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
package io.tileverse.parquetry.cli.support;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.ParquetSource;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.tileverse.ByteRangeSources;

class FixturesTest {

    @Test
    void writesReadableCitiesFixture(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("cities.parquet");
        Fixtures.writeCities(file);
        try (Storage storage = StorageFactory.open(dir.toUri());
                RangeReader reader = storage.openRangeReader("cities.parquet")) {
            ParquetSource source = ParquetSource.open(ByteRangeSources.from(reader));
            try (Stream<ParquetRecord> rows =
                    source.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                assertThat(rows.count()).isEqualTo(4L);
            }
        }
    }

    @Test
    void writesReadableGeoCitiesFixture(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("geo-cities.parquet");
        Fixtures.writeGeoCities(file);
        try (Storage storage = StorageFactory.open(dir.toUri());
                RangeReader reader = storage.openRangeReader("geo-cities.parquet")) {
            ParquetSource source = ParquetSource.open(ByteRangeSources.from(reader));
            boolean hasGeometry = source.schema().root().children().stream()
                    .filter(node -> node instanceof SchemaNode.Primitive)
                    .map(node -> (SchemaNode.Primitive) node)
                    .anyMatch(p -> "geometry".equals(p.name()));
            assertThat(hasGeometry).isTrue();
            try (Stream<ParquetRecord> rows =
                    source.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                assertThat(rows.count()).isEqualTo(2L);
            }
        }
    }
}
