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
package io.tileverse.parquetry.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
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

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.Dataset;
import io.tileverse.parquetry.dataset.DatasetCapabilities;
import io.tileverse.parquetry.dataset.GeoParquetDataset;
import io.tileverse.parquetry.dataset.ParquetDataset;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.FileEntry;
import io.tileverse.parquetry.io.FileSource;
import io.tileverse.parquetry.io.LocalFileSource;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testsupport.CorpusFixtures;

class FilesetCatalogTest {

    private static final Path FILE = CorpusFixtures.parquetTestingData().resolve("alltypes_plain.parquet");

    private long singleFileRowCount() {
        try (ByteRangeSource src = ByteRangeSource.ofFile(FILE)) {
            return ParquetDataset.open(src).count();
        }
    }

    @Test
    void singleFileDatasetReadsSchemaAndCount(@TempDir Path dir) throws Exception {
        Path only = dir.resolve("alltypes_plain.parquet");
        Files.copy(FILE, only);

        try (FilesetCatalog catalog = FilesetCatalog.open(LocalFileSource.file(only), CatalogOptions.defaults())) {
            assertThat(catalog.datasets()).containsExactly("alltypes_plain");
            Dataset ds = catalog.dataset("alltypes_plain");
            assertThat(ds.count(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS)).isEqualTo(singleFileRowCount());
        }
    }

    @Test
    void flatDirectoryConcatenatesSameSchemaFiles(@TempDir Path dir) throws Exception {
        Files.copy(FILE, dir.resolve("a.parquet"));
        Files.copy(FILE, dir.resolve("b.parquet"));

        try (FilesetCatalog catalog = FilesetCatalog.open(
                LocalFileSource.directory(dir, "*.parquet"),
                CatalogOptions.builder().datasetName("places").build())) {

            assertThat(catalog.datasets()).containsExactly("places");
            Dataset ds = catalog.dataset("places");
            assertThat(ds.count(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS)).isEqualTo(2 * singleFileRowCount());
        }
    }

    @Test
    void flatDirectoryOfPartsIsOneDataset(@TempDir Path dir) throws Exception {
        Files.copy(FILE, dir.resolve("part-0.parquet"));
        Files.copy(FILE, dir.resolve("part-1.parquet"));

        try (FilesetCatalog catalog =
                FilesetCatalog.open(LocalFileSource.directory(dir, "*.parquet"), CatalogOptions.defaults())) {
            assertThat(catalog.datasets()).hasSize(1);
            assertThat(catalog.capabilities().enumeratesDatasets()).isFalse();
        }
    }

    @Test
    void unknownDatasetNameRejected(@TempDir Path dir) throws Exception {
        Files.copy(FILE, dir.resolve("a.parquet"));
        try (FilesetCatalog catalog =
                FilesetCatalog.open(LocalFileSource.directory(dir, "*.parquet"), CatalogOptions.defaults())) {
            assertThatThrownBy(() -> catalog.dataset("nope")).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void emptySourceRejected(@TempDir Path dir) {
        LocalFileSource emptySource = LocalFileSource.directory(dir, "*.parquet");
        CatalogOptions options = CatalogOptions.defaults();
        assertThatThrownBy(() -> FilesetCatalog.open(emptySource, options))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void emptySourceIsClosedBeforeRejecting() {
        ClosingSpyFileSource source = new ClosingSpyFileSource();
        CatalogOptions options = CatalogOptions.defaults();
        assertThatThrownBy(() -> FilesetCatalog.open(source, options)).isInstanceOf(IllegalArgumentException.class);
        assertThat(source.closed).isTrue();
    }

    /** A {@link FileSource} that lists nothing and records whether it was closed. */
    private static final class ClosingSpyFileSource implements FileSource {

        private boolean closed;

        @Override
        public URI root() {
            return URI.create("memory:///empty");
        }

        @Override
        public Stream<FileEntry> list() {
            return Stream.empty();
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    @Test
    void multiFileNameDerivedFromDirectory(@TempDir Path parent) throws Exception {
        Path datasetDir = Files.createDirectory(parent.resolve("places"));
        Files.copy(FILE, datasetDir.resolve("a.parquet"));
        Files.copy(FILE, datasetDir.resolve("b.parquet"));

        try (FilesetCatalog catalog =
                FilesetCatalog.open(LocalFileSource.directory(datasetDir, "*.parquet"), CatalogOptions.defaults())) {
            assertThat(catalog.datasets()).containsExactly("places");
            Dataset ds = catalog.dataset("places");
            assertThat(ds.count(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS)).isEqualTo(2 * singleFileRowCount());
        }
    }

    @Test
    void closeReleasesSources(@TempDir Path dir) throws Exception {
        Files.copy(FILE, dir.resolve("a.parquet"));
        FilesetCatalog catalog =
                FilesetCatalog.open(LocalFileSource.directory(dir, "*.parquet"), CatalogOptions.defaults());
        catalog.close();
        // A second close must not throw: every source was already released.
        assertThatCode(catalog::close).doesNotThrowAnyException();
    }

    @Test
    void hivePartitionPruningReturnsOnlyMatchingRows(@TempDir Path root) throws Exception {
        Path part2023 = Files.createDirectories(root.resolve("year=2023"));
        Path part2024 = Files.createDirectories(root.resolve("year=2024"));
        int rows2023 = 5;
        int rows2024 = 3;
        writeYearFile(part2023.resolve("a.parquet"), 2023, rows2023);
        writeYearFile(part2024.resolve("b.parquet"), 2024, rows2024);

        try (FilesetCatalog catalog =
                FilesetCatalog.open(LocalFileSource.directory(root, "**.parquet"), CatalogOptions.defaults())) {
            Dataset ds = catalog.dataset(catalog.datasets().get(0));
            assertThat(ds.capabilities().partitionModel()).isEqualTo(DatasetCapabilities.PartitionModel.HIVE_PATH);

            Predicate year2024 = Pred.col("year").eq(2024);
            assertThat(ds.count(year2024, ReadOptions.DEFAULTS)).isEqualTo(rows2024);
        }
    }

    @Test
    void pathOnlyPartitionColumnIsSynthesized(@TempDir Path root) throws Exception {
        Path part2023 = Files.createDirectories(root.resolve("year=2023"));
        Path part2024 = Files.createDirectories(root.resolve("year=2024"));
        writeNoYearFile(part2023.resolve("a.parquet"), 4);
        writeNoYearFile(part2024.resolve("b.parquet"), 3);
        try (FilesetCatalog catalog =
                FilesetCatalog.open(LocalFileSource.directory(root, "**.parquet"), CatalogOptions.defaults())) {
            Dataset ds = catalog.dataset(catalog.datasets().get(0));
            assertThat(ds.schema().leafColumns()).contains(ColumnPath.of("year"));
            assertThat(ds.capabilities().partitionModel()).isEqualTo(DatasetCapabilities.PartitionModel.HIVE_PATH);
            assertThat(ds.count(Pred.col("year").eq(2024L), ReadOptions.DEFAULTS))
                    .isEqualTo(3);
        }
    }

    @Test
    void invalidGeoMetadataDegradesToGeometrylessType(@TempDir Path dir) throws Exception {
        Schema schema = new Schema.Parser().parse("""
                        {"type":"record","name":"Row","fields":[
                          {"name":"value","type":"double"}
                        ]}""");
        writeRows(dir.resolve("good.parquet"), schema, Map.of(), 2);
        writeRows(dir.resolve("bad.parquet"), schema, Map.of("geo", "{ this is not valid json"), 2);

        try (FilesetCatalog catalog = FilesetCatalog.open(
                LocalFileSource.directory(dir, "*.parquet"),
                CatalogOptions.builder().datasetName("places").build())) {
            Dataset ds = catalog.dataset("places");
            assertThat(ds).isInstanceOf(GeoParquetDataset.class);
            assertThat(((GeoParquetDataset) ds).geoMetadata()).isEqualTo(Optional.empty());
        }
    }

    private static void writeRows(Path out, Schema schema, Map<String, String> extraMetadata, int rowCount)
            throws IOException {
        try (ParquetWriter<GenericData.Record> writer = parquetWriter(out, schema, extraMetadata)) {
            for (int i = 0; i < rowCount; i++) {
                GenericData.Record row = new GenericData.Record(schema);
                row.put("value", i * 1.5);
                writer.write(row);
            }
        }
    }

    private static void writeYearFile(Path out, int year, int rowCount) throws IOException {
        Schema schema = new Schema.Parser().parse("""
                        {"type":"record","name":"Row","fields":[
                          {"name":"year","type":"int"},
                          {"name":"value","type":"double"}
                        ]}""");
        try (ParquetWriter<GenericData.Record> writer = parquetWriter(out, schema)) {
            for (int i = 0; i < rowCount; i++) {
                GenericData.Record row = new GenericData.Record(schema);
                row.put("year", year);
                row.put("value", i * 1.5);
                writer.write(row);
            }
        }
    }

    private static void writeNoYearFile(Path out, int rowCount) throws IOException {
        Schema schema = new Schema.Parser().parse("""
                        {"type":"record","name":"Row","fields":[
                          {"name":"value","type":"double"}
                        ]}""");
        try (ParquetWriter<GenericData.Record> writer = parquetWriter(out, schema)) {
            for (int i = 0; i < rowCount; i++) {
                GenericData.Record row = new GenericData.Record(schema);
                row.put("value", i * 1.5);
                writer.write(row);
            }
        }
    }

    private static ParquetWriter<GenericData.Record> parquetWriter(Path out, Schema schema) throws IOException {
        return parquetWriter(out, schema, Map.of());
    }

    private static ParquetWriter<GenericData.Record> parquetWriter(
            Path out, Schema schema, Map<String, String> extraMetadata) throws IOException {
        return AvroParquetWriter.<GenericData.Record>builder(new LocalOutputFile(out))
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriterVersion(WriterVersion.PARQUET_2_0)
                .withExtraMetaData(extraMetadata)
                .build();
    }
}
