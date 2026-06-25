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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
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
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.LocalFileSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * A Hive tree where {@code region} is a physical partition column (its value is also a column inside every data file)
 * and {@code year} is path-only (no file holds it). The dataset schema must expose both, pruning must honor both, and a
 * read must project the synthesized {@code year} alongside the physical {@code region} with correct per-row values.
 */
class MixedPartitionTreeTest {

    private static final ColumnPath YEAR = ColumnPath.of("year");
    private static final ColumnPath REGION = ColumnPath.of("region");

    @Test
    void schemaExposesBothSyntheticYearAndPhysicalRegion(@TempDir Path root) throws Exception {
        writeMixedTree(root);
        try (FilesetCatalog catalog =
                FilesetCatalog.open(LocalFileSource.directory(root, "**.parquet"), CatalogOptions.defaults())) {
            ParquetDataset ds = catalog.dataset(catalog.datasets().get(0));

            assertThat(ds.schema().leafColumns()).contains(YEAR, REGION);
            assertThat(ds.capabilities().partitionModel()).isEqualTo(DatasetCapabilities.PartitionModel.HIVE_PATH);
        }
    }

    @Test
    void filterOnBothColumnsCountsOnlyMatchingRows(@TempDir Path root) throws Exception {
        writeMixedTree(root);
        try (FilesetCatalog catalog =
                FilesetCatalog.open(LocalFileSource.directory(root, "**.parquet"), CatalogOptions.defaults())) {
            ParquetDataset ds = catalog.dataset(catalog.datasets().get(0));
            Predicate year2024West =
                    Pred.and(Pred.col("year").eq(2024L), Pred.col("region").eq("west"));

            assertThat(ds.count(year2024West, ReadOptions.DEFAULTS)).isEqualTo(WEST_2024_ROWS);
        }
    }

    @Test
    void readProjectsSynthesizedYearAndPhysicalRegionPerRow(@TempDir Path root) throws Exception {
        writeMixedTree(root);
        try (FilesetCatalog catalog =
                FilesetCatalog.open(LocalFileSource.directory(root, "**.parquet"), CatalogOptions.defaults())) {
            ParquetDataset ds = catalog.dataset(catalog.datasets().get(0));
            Predicate year2024West =
                    Pred.and(Pred.col("year").eq(2024L), Pred.col("region").eq("west"));

            List<ParquetRecord> rows = readAll(ds, year2024West);

            assertThat(rows).hasSize(WEST_2024_ROWS);
            assertThat(rows).allSatisfy(row -> {
                assertThat(row.getLong(YEAR)).isEqualTo(2024L);
                assertThat(row.getString(REGION)).isEqualTo("west");
            });
        }
    }

    private static List<ParquetRecord> readAll(ParquetDataset ds, Predicate predicate) {
        try (Stream<ParquetRecord> records = ds.read(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
            return records.toList();
        }
    }

    private static final int WEST_2024_ROWS = 3;
    private static final int EAST_2024_ROWS = 2;
    private static final int WEST_2023_ROWS = 4;

    private static void writeMixedTree(Path root) throws IOException {
        writeRegionFile(root.resolve("year=2024/region=west/part.parquet"), "west", WEST_2024_ROWS);
        writeRegionFile(root.resolve("year=2024/region=east/part.parquet"), "east", EAST_2024_ROWS);
        writeRegionFile(root.resolve("year=2023/region=west/part.parquet"), "west", WEST_2023_ROWS);
    }

    private static void writeRegionFile(Path out, String region, int rowCount) throws IOException {
        Files.createDirectories(out.getParent());
        Schema schema = new Schema.Parser().parse("""
                        {"type":"record","name":"Row","fields":[
                          {"name":"region","type":"string"},
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
                row.put("region", region);
                row.put("value", i * 1.5);
                writer.write(row);
            }
        }
    }
}
