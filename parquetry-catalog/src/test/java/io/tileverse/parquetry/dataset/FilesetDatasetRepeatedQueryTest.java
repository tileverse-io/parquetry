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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
 * Proves that a multi-file {@link FilesetDataset} answers repeated queries identically: each query opens the surviving
 * files afresh (pinned by FilesetCatalogTest's per-call footer-decode test) and two sequential queries over the same
 * dataset return the same results.
 */
class FilesetDatasetRepeatedQueryTest {

    @Test
    void twoSequentialQueriesReturnIdenticalResults(@TempDir Path root) throws Exception {
        writeYearTree(root, 2023, 5, 2024, 3);
        try (FilesetCatalog catalog =
                FilesetCatalog.open(LocalFileSource.directory(root, "**.parquet"), CatalogOptions.defaults())) {
            FilesetDataset dataset =
                    (FilesetDataset) catalog.dataset(catalog.datasets().get(0));
            Predicate onSyntheticColumn = Pred.col("year").eq(2024L);

            long first = countMatching(dataset, onSyntheticColumn);
            long second = countMatching(dataset, onSyntheticColumn);

            assertThat(first).isEqualTo(3L);
            assertThat(second).isEqualTo(first);
        }
    }

    private static long countMatching(FilesetDataset dataset, Predicate predicate) {
        try (Stream<ParquetRecord> records = dataset.read(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
            return records.map(row -> row.getLong(ColumnPath.of("year")))
                    .filter(year -> year == 2024L)
                    .count();
        }
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
