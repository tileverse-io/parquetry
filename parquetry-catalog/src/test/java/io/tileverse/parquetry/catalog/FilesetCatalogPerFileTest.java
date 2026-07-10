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
package io.tileverse.parquetry.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.URI;
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

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.ParquetDataset;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.FileEntry;
import io.tileverse.parquetry.io.FileSource;
import io.tileverse.parquetry.io.LocalFileSource;
import io.tileverse.parquetry.schema.ColumnPath;

class FilesetCatalogPerFileTest {

    @Test
    void distinctSchemaFilesBecomeSeparateDatasets(@TempDir Path dir) throws Exception {
        writeIdNameFile(dir.resolve("ne_countries.parquet"), 2);
        writeValueFile(dir.resolve("ne_rivers.parquet"), 3);

        try (FilesetCatalog catalog = FilesetCatalog.openPerFile(LocalFileSource.directory(dir, "*.parquet"))) {
            assertThat(catalog.datasets()).containsExactly("ne_countries", "ne_rivers");

            ParquetDataset countries = catalog.dataset("ne_countries");
            assertThat(countries.schema().leafColumns()).contains(ColumnPath.of("name"));
            assertThat(countries.count(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS))
                    .isEqualTo(2);

            ParquetDataset rivers = catalog.dataset("ne_rivers");
            assertThat(rivers.schema().leafColumns()).contains(ColumnPath.of("value"));
            assertThat(rivers.count(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS))
                    .isEqualTo(3);
        }
    }

    @Test
    void advertisesDatasetEnumeration(@TempDir Path dir) throws Exception {
        writeIdNameFile(dir.resolve("a.parquet"), 1);
        try (FilesetCatalog catalog = FilesetCatalog.openPerFile(LocalFileSource.directory(dir, "*.parquet"))) {
            assertThat(catalog.capabilities().enumeratesDatasets()).isTrue();
        }
    }

    @Test
    void unknownDatasetNameRejected(@TempDir Path dir) throws Exception {
        writeIdNameFile(dir.resolve("a.parquet"), 1);
        try (FilesetCatalog catalog = FilesetCatalog.openPerFile(LocalFileSource.directory(dir, "*.parquet"))) {
            assertThatThrownBy(() -> catalog.dataset("nope"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("nope");
        }
    }

    @Test
    void emptySourceRejected(@TempDir Path dir) {
        LocalFileSource emptySource = LocalFileSource.directory(dir, "*.parquet");
        assertThatThrownBy(() -> FilesetCatalog.openPerFile(emptySource))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no files found");
    }

    @Test
    void emptySourceIsClosedBeforeRejecting() {
        StubFileSource source = new StubFileSource(List.of());
        assertThatThrownBy(() -> FilesetCatalog.openPerFile(source)).isInstanceOf(IllegalArgumentException.class);
        assertThat(source.closed).isTrue();
    }

    @Test
    void collidingDatasetNamesFailFast(@TempDir Path dir) throws Exception {
        Path backing = dir.resolve("data.parquet");
        writeIdNameFile(backing, 1);
        StubFileSource nested =
                new StubFileSource(List.of(entry("a/data.parquet", backing), entry("b/data.parquet", backing)));

        assertThatThrownBy(() -> FilesetCatalog.openPerFile(nested))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("a/data.parquet")
                .hasMessageContaining("b/data.parquet");
        assertThat(nested.closed).isTrue();
    }

    @Test
    void closeReleasesSources(@TempDir Path dir) throws Exception {
        writeIdNameFile(dir.resolve("a.parquet"), 1);
        FilesetCatalog catalog = FilesetCatalog.openPerFile(LocalFileSource.directory(dir, "*.parquet"));
        catalog.close();
        // A second close must not throw: every source was already released.
        assertThatCode(catalog::close).doesNotThrowAnyException();
    }

    private static FileEntry entry(String relativePath, Path backing) {
        return new FileEntry() {
            @Override
            public String relativePath() {
                return relativePath;
            }

            @Override
            public long sizeBytes() {
                return -1;
            }

            @Override
            public ByteRangeSource open() {
                return ByteRangeSource.ofFile(backing);
            }
        };
    }

    /** A {@link FileSource} over fixed entries that records whether it was closed. */
    private static final class StubFileSource implements FileSource {

        private final List<FileEntry> entries;
        private boolean closed;

        StubFileSource(List<FileEntry> entries) {
            this.entries = entries;
        }

        @Override
        public URI root() {
            return URI.create("memory:///stub");
        }

        @Override
        public Stream<FileEntry> list() {
            return entries.stream();
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static void writeIdNameFile(Path out, int rowCount) throws IOException {
        Schema schema = new Schema.Parser().parse("""
                        {"type":"record","name":"Row","fields":[
                          {"name":"id","type":"int"},
                          {"name":"name","type":"string"}
                        ]}""");
        try (ParquetWriter<GenericData.Record> writer = parquetWriter(out, schema)) {
            for (int i = 0; i < rowCount; i++) {
                GenericData.Record row = new GenericData.Record(schema);
                row.put("id", i);
                row.put("name", "name-" + i);
                writer.write(row);
            }
        }
    }

    private static void writeValueFile(Path out, int rowCount) throws IOException {
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
        return AvroParquetWriter.<GenericData.Record>builder(new LocalOutputFile(out))
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriterVersion(WriterVersion.PARQUET_2_0)
                .build();
    }
}
