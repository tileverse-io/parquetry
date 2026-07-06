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
package io.tileverse.parquetry.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.ParquetDataset;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * A multi-file read consumed through {@code Stream.iterator()} must deliver exactly the rows an in-pipeline consumption
 * delivers. The rows are flyweight views valid until the next pull; the iterator adapter's pull pattern is the sharpest
 * consumer of that contract, and a flatten that released a batch's memory before its rows were delivered corrupts
 * values silently when the recycled bytes still decode. Comparing per-id geometry bytes between the two consumption
 * styles catches that corruption as a content difference.
 */
class IcebergIteratorConsumptionTest {

    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath GEOM = ColumnPath.of("geom");

    @Test
    void iteratorConsumptionDeliversTheSameRowsAsInPipelineConsumption(@TempDir java.nio.file.Path tempDir) {
        java.nio.file.Path root = TestCorpus.extractDirectory("iceberg-geo-testbed", tempDir.resolve("corpus"));
        try (IcebergTableCatalog catalog =
                IcebergTableCatalog.openLocal(root.resolve("v3_geometry"), IcebergOptions.defaults())) {
            ParquetDataset dataset = catalog.dataset("v3_geometry");

            Map<String, Integer> inPipeline = new HashMap<>();
            try (Stream<ParquetRecord> rows =
                    dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                rows.forEach(row -> inPipeline.put(row.getString(ID), Arrays.hashCode(row.getBinary(GEOM))));
            }
            assertThat(inPipeline).hasSize(10_000);

            Map<String, Integer> iterated = new HashMap<>();
            try (Stream<ParquetRecord> rows =
                    dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                Iterator<ParquetRecord> it = rows.iterator();
                while (it.hasNext()) {
                    ParquetRecord row = it.next();
                    iterated.put(row.getString(ID), Arrays.hashCode(row.getBinary(GEOM)));
                }
            }

            assertThat(iterated).isEqualTo(inPipeline);
        }
    }
}
