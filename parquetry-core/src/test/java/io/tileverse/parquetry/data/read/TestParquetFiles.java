/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.data.read;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.avro.generic.GenericData;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;

import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.io.ByteRangeSource;

/**
 * Shared test-fixture helpers for Parquet read-path tests.
 *
 * <p>Provides a canonical three-column multi-row-group file writer, a row-group counter, and a {@link ByteRangeSource}
 * factory over a local file.
 */
final class TestParquetFiles {

    private TestParquetFiles() {}

    private static final String SCHEMA = """
            {"type":"record","name":"Row","fields":[
              {"name":"year","type":"int"},
              {"name":"country","type":"string"},
              {"name":"value","type":"double"}
            ]}""";

    /**
     * Writes a Parquet 2.0 / Snappy file with three columns ({@code year}, {@code country}, {@code value}) and a tiny
     * row-group target so the given number of rows spans multiple row groups.
     *
     * @param dir directory in which to write the file
     * @param rows total number of rows to write
     * @return path to the written file
     */
    static Path writeFlatThreeColumnFileMultiRowGroup(Path dir, int rows) throws IOException {
        Path file = dir.resolve("three-col-multi-rg.parquet");
        org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(SCHEMA);
        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(
                        new LocalOutputFile(file))
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriterVersion(WriterVersion.PARQUET_2_0)
                .withRowGroupSize(8_192L)
                .build()) {
            for (int i = 0; i < rows; i++) {
                GenericData.Record rec = new GenericData.Record(schema);
                rec.put("year", 2020 + (i % 5));
                rec.put("country", (i % 2 == 0) ? "AR" : "BR");
                rec.put("value", i * 1.5);
                writer.write(rec);
            }
        }
        return file;
    }

    /**
     * Returns the number of row groups in the given Parquet file.
     *
     * <p>Opens a fresh source for the sole purpose of reading the footer, then closes it before returning.
     */
    static int rowGroupCount(Path file) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            return ParquetFormat.readFooter(source).rowGroups().size();
        }
    }

    /** Opens a {@link ByteRangeSource} over the local {@code file}; closing it closes the underlying channel. */
    static ByteRangeSource openRangeReader(Path file) {
        return ByteRangeSource.ofFile(file);
    }
}
