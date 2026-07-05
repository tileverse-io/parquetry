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
package io.tileverse.parquetry.stac;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.OpenOptions;
import io.tileverse.parquetry.dataset.explain.DatasetExplainPlan;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.tileverse.ByteRangeSources;

class StacDatasetTest {

    @Test
    void bboxQuerySkipsDisjointPartsAndReadsSameRowsAsFullScan(@TempDir Path dir) throws Exception {
        StacPointParquet.writePoints(dir.resolve("west.parquet"), "geometry", new double[][] {{0, 0}, {5, 5}});
        StacPointParquet.writePoints(dir.resolve("mid.parquet"), "geometry", new double[][] {{50, 0}, {55, 5}});
        StacPointParquet.writePoints(dir.resolve("east.parquet"), "geometry", new double[][] {{100, 0}, {105, 5}});

        try (Storage storage = StorageFactory.open(dir.toUri())) {
            List<ByteRangeSource> sources = openAll(storage, "west.parquet", "mid.parquet", "east.parquet");
            List<StacItemRef> refs = List.of(
                    new StacItemRef("west", "west.parquet"),
                    new StacItemRef("mid", "mid.parquet"),
                    new StacItemRef("east", "east.parquet"));
            List<double[]> bboxes =
                    List.of(new double[] {0, 0, 5, 5}, new double[] {50, 0, 55, 5}, new double[] {100, 0, 105, 5});

            StacDataset dataset = new StacDataset("building", "geometry", refs, bboxes, sources, OpenOptions.DEFAULTS);

            Predicate eastOnly =
                    new Predicate.Spatial.BboxIntersects(ColumnPath.of("geometry"), Bbox.of2d(95, -5, 115, 15));

            DatasetExplainPlan plan = dataset.explain(eastOnly, Projection.ALL, ReadOptions.DEFAULTS);
            assertThat(plan.totals().filesSkipped()).isEqualTo(2);
            assertThat(plan.totals().filesKept()).isEqualTo(1);

            assertThat(dataset.count(eastOnly, ReadOptions.DEFAULTS)).isEqualTo(2);

            long full = countRows(dataset, Predicate.ALWAYS_TRUE);
            assertThat(full).isEqualTo(6);
            long filtered = countRows(dataset, eastOnly);
            assertThat(filtered).isEqualTo(2);

            closeAll(sources);
        }
    }

    private static long countRows(StacDataset dataset, Predicate predicate) {
        try (Stream<ParquetRecord> rows = dataset.read(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
            return rows.count();
        }
    }

    private static List<ByteRangeSource> openAll(Storage storage, String... keys) {
        return Stream.of(keys)
                .map(key -> ByteRangeSources.from(storage.openRangeReader(key)))
                .toList();
    }

    private static void closeAll(List<ByteRangeSource> sources) {
        for (ByteRangeSource source : sources) {
            source.close();
        }
    }
}
