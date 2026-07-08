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
package io.tileverse.parquetry.stac;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.OpenOptions;
import io.tileverse.parquetry.dataset.explain.DatasetExplainPlan;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.runtime.ParquetRuntime;
import io.tileverse.parquetry.schema.ColumnPath;

import io.tileverse.stac.StacFormatException;

class StacDatasetTest {

    @Test
    void bboxQuerySkipsDisjointPartsAndReadsSameRowsAsFullScan(@TempDir Path dir) throws Exception {
        StacPointParquet.writePoints(dir.resolve("west.parquet"), "geometry", new double[][] {{0, 0}, {5, 5}});
        StacPointParquet.writePoints(dir.resolve("mid.parquet"), "geometry", new double[][] {{50, 0}, {55, 5}});
        StacPointParquet.writePoints(dir.resolve("east.parquet"), "geometry", new double[][] {{100, 0}, {105, 5}});

        try (ContainerStorages storages = new ContainerStorages(new Properties())) {
            List<StacItemRef> refs = List.of(
                    new StacItemRef("west", dir.resolve("west.parquet").toUri().toString()),
                    new StacItemRef("mid", dir.resolve("mid.parquet").toUri().toString()),
                    new StacItemRef("east", dir.resolve("east.parquet").toUri().toString()));
            List<double[]> bboxes =
                    List.of(new double[] {0, 0, 5, 5}, new double[] {50, 0, 55, 5}, new double[] {100, 0, 105, 5});

            StacDataset dataset = new StacDataset("building", "geometry", refs, bboxes, storages, OpenOptions.DEFAULTS);
            try {
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
            } finally {
                dataset.closeResources();
            }
        }
    }

    @Test
    void readingAHeterogeneousCollectionFailsLoud(@TempDir Path tempDir) throws Exception {
        Path dir = Files.createDirectories(tempDir.resolve("data"));
        Path part0 = dir.resolve("p0.parquet");
        Path part1 = dir.resolve("p1.parquet");
        StacPointParquet.writePoints(part0, "geometry", new double[][] {{0, 0}});
        StacPointParquet.writePoints(part1, "geom_other", new double[][] {{1, 1}});

        try (ContainerStorages storages = new ContainerStorages(new Properties())) {
            StacDataset dataset = new StacDataset(
                    "buildings",
                    "geometry",
                    List.of(
                            new StacItemRef("i0", part0.toUri().toString()),
                            new StacItemRef("i1", part1.toUri().toString())),
                    List.of(new double[] {0, 0, 0, 0}, new double[] {1, 1, 1, 1}),
                    storages,
                    OpenOptions.DEFAULTS);
            try {
                assertThatThrownBy(() -> {
                            try (Stream<ParquetRecord> rows =
                                    dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                                rows.count();
                            }
                        })
                        .isInstanceOf(StacFormatException.class)
                        .hasMessageContaining("i1")
                        .hasMessageContaining("schema");
            } finally {
                dataset.closeResources();
            }
        }
    }

    @Test
    void aFailingPartReadRepeatedlyDoesNotAccumulateOpenReaders(@TempDir Path dir) throws Exception {
        StacPointParquet.writePoints(dir.resolve("good.parquet"), "geometry", new double[][] {{0, 0}});
        Path corrupt = dir.resolve("corrupt.parquet");
        Files.write(corrupt, "not a parquet file".getBytes(StandardCharsets.UTF_8));

        try (ContainerStorages storages = new ContainerStorages(new Properties())) {
            StacDataset dataset = new StacDataset(
                    "buildings",
                    "geometry",
                    List.of(
                            new StacItemRef(
                                    "good", dir.resolve("good.parquet").toUri().toString()),
                            new StacItemRef("corrupt", corrupt.toUri().toString())),
                    List.of(new double[] {0, 0, 0, 0}, new double[] {1, 1, 1, 1}),
                    storages,
                    OpenOptions.DEFAULTS);
            try {
                readExpectingFailure(dataset);
                int afterFirst = dataset.openReaderCount();
                readExpectingFailure(dataset);
                int afterSecond = dataset.openReaderCount();
                assertThat(afterSecond).isEqualTo(afterFirst);
            } finally {
                dataset.closeResources();
            }
        }
    }

    private static void readExpectingFailure(StacDataset dataset) {
        assertThatThrownBy(() -> {
                    try (Stream<ParquetRecord> rows =
                            dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                        rows.count();
                    }
                })
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void aReadStreamDoesNotOpenEverySurvivorFooterUpFront(@TempDir Path dir) throws Exception {
        int parts = 5;
        List<StacItemRef> refs = new ArrayList<>();
        List<double[]> bboxes = new ArrayList<>();
        for (int part = 0; part < parts; part++) {
            Path file = dir.resolve("part" + part + ".parquet");
            StacPointParquet.writePoints(file, "geometry", new double[][] {{part, part}});
            refs.add(new StacItemRef("i" + part, file.toUri().toString()));
            bboxes.add(new double[] {part, part, part, part});
        }
        OpenOptions oneAtATime = OpenOptions.builder()
                .runtime(ParquetRuntime.builder().maxConcurrentFiles(1).build())
                .build();

        try (ContainerStorages storages = new ContainerStorages(new Properties())) {
            StacDataset dataset = new StacDataset("buildings", "geometry", refs, bboxes, storages, oneAtATime);
            try {
                try (Stream<ParquetRecord> rows =
                        dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                    // intentionally not consumed: the pre-fix guard opened every survivor's footer at this point
                }
                assertThat(dataset.openReaderCount()).isLessThan(parts);
            } finally {
                dataset.closeResources();
            }
        }
    }

    private static long countRows(StacDataset dataset, Predicate predicate) {
        try (Stream<ParquetRecord> rows = dataset.read(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
            return rows.count();
        }
    }
}
