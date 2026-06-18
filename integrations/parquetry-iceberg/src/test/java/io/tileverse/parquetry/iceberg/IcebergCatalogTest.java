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
package io.tileverse.parquetry.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.Dataset;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testkit.TestCorpus;

class IcebergCatalogTest {

    @TempDir
    Path tempDir;

    @ParameterizedTest
    @ValueSource(
            strings = {
                "v2_flat_columns",
                "v2_bbox_struct",
                "v2_geo_convention",
                "v3_geometry",
                "v3_geometry_lineage",
                "v3_minimal"
            })
    void fullScanReadsEveryRow(String table) {
        Path root = TestCorpus.extractDirectory("iceberg-geo-testbed", tempDir.resolve(table));
        try (IcebergCatalog catalog = IcebergCatalog.openLocal(root.resolve(table), IcebergOptions.defaults())) {
            assertThat(catalog.datasets()).containsExactly(table);
            Dataset dataset = catalog.dataset(table);
            long count = dataset.count(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS);
            assertThat(count).isEqualTo(10_000L);

            long decoded;
            try (Stream<ParquetRecord> rows =
                    dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                decoded = rows.count();
            }
            assertThat(decoded).isEqualTo(10_000L);
        }
    }

    @Test
    void spatialPredicateReturnsCorrectRows() {
        Path root = TestCorpus.extractDirectory("iceberg-geo-testbed", tempDir.resolve("v3_geometry"));
        try (IcebergCatalog catalog =
                IcebergCatalog.openLocal(root.resolve("v3_geometry"), IcebergOptions.defaults())) {
            Dataset dataset = catalog.dataset("v3_geometry");
            Bbox california = Bbox.of2d(-125.0, 32.0, -115.0, 42.0);
            Predicate predicate = new Predicate.Spatial.BboxIntersects(ColumnPath.of("geom"), california);

            long pushedDown;
            try (Stream<ParquetRecord> rows = dataset.read(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
                pushedDown = rows.count();
            }

            long oracle = bruteForceInWindow(dataset, california);
            assertThat(pushedDown).isEqualTo(oracle);
            assertThat(pushedDown).isGreaterThan(0L);
        }
    }

    private static long bruteForceInWindow(Dataset dataset, Bbox window) {
        long inside = 0L;
        try (Stream<ParquetRecord> rows = dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
            for (ParquetRecord row : (Iterable<ParquetRecord>) rows::iterator) {
                if (pointInWindow(row, window)) {
                    inside++;
                }
            }
        }
        return inside;
    }

    private static boolean pointInWindow(ParquetRecord row, Bbox window) {
        byte[] wkb = row.getBinary(ColumnPath.of("geom"));
        ByteBuffer buffer = ByteBuffer.wrap(wkb).order(ByteOrder.LITTLE_ENDIAN);
        // Little-endian WKB point: 1-byte byte-order flag + 4-byte geometry type, then the X and Y doubles.
        double x = buffer.getDouble(5);
        double y = buffer.getDouble(13);
        return x >= window.minX() && x <= window.maxX() && y >= window.minY() && y <= window.maxY();
    }
}
