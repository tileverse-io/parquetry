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
package io.tileverse.parquetry.avro;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Verifies that the reference Avro implementation reads back exactly what this module's {@link AvroDataFileWriter}
 * produces. Two angles: every supported codec round-trips a small record through avro-java, and a vendored corpus file
 * survives a parquetry read then a parquetry rewrite that avro-java reads field-for-field equal to the originals.
 */
class AvroWriteOracleTest {

    private static final String SCHEMA = "{\"type\":\"record\",\"name\":\"R\",\"namespace\":\"x\",\"fields\":["
            + "{\"name\":\"id\",\"type\":\"long\"},"
            + "{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"items\":\"string\"}},"
            + "{\"name\":\"props\",\"type\":{\"type\":\"map\",\"values\":\"int\"}}]}";

    @TempDir
    Path tempDir;

    @ParameterizedTest
    @ValueSource(strings = {"null", "deflate", "snappy", "zstandard", "bzip2", "xz"})
    void apacheAvroReadsWhatWeWrite(String codec) throws IOException {
        byte[] file = writeSmallRecord(codec);

        List<GenericRecord> read = readWithAvroJava(file, SCHEMA);

        assertThat(read).hasSize(1);
        GenericRecord record = read.get(0);
        assertThat(record.get("id")).isEqualTo(7L);
        assertThat(normalizeField(record, "tags")).isEqualTo(List.of("a", "b"));
        assertThat(normalizeField(record, "props")).isEqualTo(Map.of("k", 1));
    }

    @Test
    void corpusRoundTripReadsBackThroughAvroJava() throws IOException {
        Path original = TestCorpus.extractFile("avro-reference/weather.avro", tempDir);

        String schemaJson = null;
        List<AvroRecord> originals = null;
        try (AvroDataFileReader reader =
                AvroDataFileReader.open(new InMemoryByteRangeSource(Files.readAllBytes(original)))) {
            schemaJson = reader.metadata().get("avro.schema");
            originals = reader.records().toList();
        }

        byte[] rewrite = rewriteWithOurWriter(schemaJson, originals);

        List<GenericRecord> reread = readWithAvroJava(rewrite, schemaJson);

        assertThat(reread).hasSize(originals.size());
        for (int i = 0; i < originals.size(); i++) {
            Object expected = OracleValueNormalizer.normalize(originals.get(i));
            Object actual = OracleValueNormalizer.normalize(reread.get(i));
            assertThat(actual).as("record %d", i).isEqualTo(expected);
        }
    }

    private static byte[] writeSmallRecord(String codec) throws IOException {
        InMemoryByteSink sink = new InMemoryByteSink();
        try (AvroDataFileWriter writer =
                AvroDataFileWriter.builder(SCHEMA).codec(codec).build(sink)) {
            writer.write(Map.of("id", 7L, "tags", List.of("a", "b"), "props", Map.of("k", 1)));
        }
        return sink.toByteArray();
    }

    private static byte[] rewriteWithOurWriter(String schemaJson, List<AvroRecord> records) throws IOException {
        InMemoryByteSink sink = new InMemoryByteSink();
        try (AvroDataFileWriter writer = AvroDataFileWriter.builder(schemaJson).build(sink)) {
            writer.write(records);
        }
        return sink.toByteArray();
    }

    private static List<GenericRecord> readWithAvroJava(byte[] file, String schemaJson) throws IOException {
        Schema schema = new Schema.Parser().parse(schemaJson);
        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        List<GenericRecord> records = new ArrayList<>();
        try (DataFileReader<GenericRecord> reader =
                new DataFileReader<>(new SeekableByteArrayInput(file), datumReader)) {
            for (GenericRecord row : reader) {
                records.add(row);
            }
        }
        return records;
    }

    private static Object normalizeField(GenericRecord record, String field) {
        return OracleValueNormalizer.normalize(record.get(field));
    }
}
