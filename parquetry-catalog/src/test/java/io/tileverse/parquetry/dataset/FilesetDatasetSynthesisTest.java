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
import static org.assertj.core.api.Assertions.assertThatCode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.catalog.CatalogOptions;
import io.tileverse.parquetry.catalog.FilesetCatalog;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.explain.DatasetExplainPlan;
import io.tileverse.parquetry.dataset.explain.FileExplain;
import io.tileverse.parquetry.dataset.explain.Outcome;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.LocalFileSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;

class FilesetDatasetSynthesisTest {

    @Test
    void readsSynthesizedValuePerFile(@TempDir Path root) throws Exception {
        writeYearTree(root, 2023, 4, 2024, 3);
        try (FilesetCatalog catalog =
                FilesetCatalog.open(LocalFileSource.directory(root, "**.parquet"), CatalogOptions.defaults())) {
            Dataset ds = catalog.dataset(catalog.datasets().get(0));
            try (Stream<ParquetRecord> records = ds.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                long count2023 = records.map(row -> row.getLong(ColumnPath.of("year")))
                        .filter(year -> year == 2023L)
                        .count();
                assertThat(count2023).isEqualTo(4L);
            }
            try (Stream<ParquetRecord> records = ds.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                long count2024 = records.map(row -> row.getLong(ColumnPath.of("year")))
                        .filter(year -> year == 2024L)
                        .count();
                assertThat(count2024).isEqualTo(3L);
            }
        }
    }

    @Test
    void projectingOnlyTheSyntheticColumnEnumeratesEveryRow(@TempDir Path root) throws Exception {
        writeYearTree(root, 2023, 4, 2024, 3);
        try (FilesetCatalog catalog =
                FilesetCatalog.open(LocalFileSource.directory(root, "**.parquet"), CatalogOptions.defaults())) {
            Dataset ds = catalog.dataset(catalog.datasets().get(0));
            Projection onlyYear = Projection.of(Set.of(ColumnPath.of("year")));
            try (Stream<ParquetRecord> records = ds.read(Predicate.ALWAYS_TRUE, onlyYear, ReadOptions.DEFAULTS)) {
                List<Long> years =
                        records.map(row -> row.getLong(ColumnPath.of("year"))).toList();
                assertThat(years).hasSize(7);
                assertThat(years.stream().filter(year -> year == 2023L).count()).isEqualTo(4L);
                assertThat(years.stream().filter(year -> year == 2024L).count()).isEqualTo(3L);
            }
        }
    }

    @Test
    void filterOnSyntheticColumnReturnsOnlyMatchingRows(@TempDir Path root) throws Exception {
        writeYearTree(root, 2023, 5, 2024, 3);
        try (FilesetCatalog catalog =
                FilesetCatalog.open(LocalFileSource.directory(root, "**.parquet"), CatalogOptions.defaults())) {
            Dataset ds = catalog.dataset(catalog.datasets().get(0));
            Predicate year2024 = Pred.col("year").eq(2024L);
            try (Stream<ParquetRecord> records = ds.read(year2024, Projection.ALL, ReadOptions.DEFAULTS)) {
                assertThat(records.count()).isEqualTo(3L);
            }
        }
    }

    @Test
    void filterCombiningSyntheticAndPhysicalAppliesRecordLevelResidual(@TempDir Path root) throws Exception {
        writeYearTree(root, 2023, 4, 2024, 10);
        try (FilesetCatalog catalog =
                FilesetCatalog.open(LocalFileSource.directory(root, "**.parquet"), CatalogOptions.defaults())) {
            Dataset ds = catalog.dataset(catalog.datasets().get(0));
            Predicate year2024AndValueOver5 =
                    Pred.and(Pred.col("year").eq(2024L), Pred.col("value").gt(5.0));

            try (Stream<ParquetRecord> records = ds.read(year2024AndValueOver5, Projection.ALL, ReadOptions.DEFAULTS)) {
                List<Double> values = records.map(row -> row.getDouble(ColumnPath.of("value")))
                        .toList();
                assertThat(values).isNotEmpty().allMatch(value -> value > 5.0);
                assertThat(values).hasSize(6);
            }

            long matching = ds.count(year2024AndValueOver5, ReadOptions.DEFAULTS);
            assertThat(matching).isEqualTo(6L);
        }
    }

    @Test
    void explainOnSyntheticColumnKeepsAndSkipsByPartitionValue(@TempDir Path root) throws Exception {
        writeYearTree(root, 2023, 5, 2024, 3);
        try (FilesetCatalog catalog =
                FilesetCatalog.open(LocalFileSource.directory(root, "**.parquet"), CatalogOptions.defaults())) {
            Dataset ds = catalog.dataset(catalog.datasets().get(0));
            Predicate year2024 = Pred.col("year").eq(2024L);

            DatasetExplainPlan plan = ds.explain(year2024, Projection.ALL, ReadOptions.DEFAULTS);

            assertThat(keptLocations(plan)).anyMatch(location -> location.contains("year=2024"));
            assertThat(skippedLocations(plan)).anyMatch(location -> location.contains("year=2023"));
            assertThatCode(() -> ds.explainAnalyze(year2024, Projection.ALL, ReadOptions.DEFAULTS))
                    .doesNotThrowAnyException();
        }
    }

    @Test
    void earlyCloseAcrossSyntheticSurvivorsDoesNotThrow(@TempDir Path root) throws Exception {
        writeYearTree(root, 2023, 5, 2024, 4);
        try (FilesetCatalog catalog =
                FilesetCatalog.open(LocalFileSource.directory(root, "**.parquet"), CatalogOptions.defaults())) {
            Dataset ds = catalog.dataset(catalog.datasets().get(0));
            assertThatCode(() -> consumeOneThenClose(ds)).doesNotThrowAnyException();
        }
    }

    private static void consumeOneThenClose(Dataset ds) {
        try (Stream<ParquetRecord> records = ds.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
            Iterator<ParquetRecord> it = records.iterator();
            assertThat(it.hasNext()).isTrue();
            it.next();
        }
    }

    private static Stream<String> keptLocations(DatasetExplainPlan plan) {
        return plan.files().stream()
                .filter(file -> file.outcome() == Outcome.KEEP)
                .map(FileExplain::location);
    }

    private static Stream<String> skippedLocations(DatasetExplainPlan plan) {
        return plan.files().stream()
                .filter(file -> file.outcome() == Outcome.SKIP)
                .map(FileExplain::location);
    }

    private static void writeYearTree(Path root, int firstYear, int firstRows, int secondYear, int secondRows)
            throws IOException {
        Path first = Files.createDirectories(root.resolve("year=" + firstYear));
        Path second = Files.createDirectories(root.resolve("year=" + secondYear));
        writeValueOnlyFile(first.resolve("a.parquet"), firstRows);
        writeValueOnlyFile(second.resolve("b.parquet"), secondRows);
    }

    private static void writeValueOnlyFile(Path out, int rowCount) throws IOException {
        Schema schema = new Schema.Parser().parse("""
                        {"type":"record","name":"Row","fields":[
                          {"name":"value","type":"double"}
                        ]}""");
        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(
                        new LocalOutputFile(out))
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriterVersion(WriterVersion.PARQUET_2_0)
                .build()) {
            for (int i = 0; i < rowCount; i++) {
                GenericData.Record row = new GenericData.Record(schema);
                row.put("value", i * 1.5);
                writer.write(row);
            }
        }
    }
}
