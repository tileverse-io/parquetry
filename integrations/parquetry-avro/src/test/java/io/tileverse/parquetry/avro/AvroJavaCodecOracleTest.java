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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Reads OCF files produced by the reference Avro implementation across every supported codec, validating the codec
 * framing (snappy's trailing CRC32, zstd frames written in streaming mode without a content size) against avro-java
 * rather than against this module's own test writer.
 */
class AvroJavaCodecOracleTest {

    private static final int ROWS = 500;

    @ParameterizedTest
    @ValueSource(strings = {"null", "deflate", "snappy", "zstandard", "bzip2", "xz"})
    void readsAvroJavaWrittenFiles(String codecName) throws IOException {
        byte[] file = writeWithAvroJava(codecName);

        try (AvroDataFileReader reader = AvroDataFileReader.open(new InMemoryByteRangeSource(file))) {
            assertThat(reader.metadata()).containsEntry("avro.codec", codecName.equals("null") ? "null" : codecName);

            List<AvroRecord> records = reader.records().toList();
            assertThat(records).hasSize(ROWS);
            for (int i = 0; i < ROWS; i++) {
                AvroRecord row = records.get(i);
                assertThat(row.get("id")).isEqualTo((long) i);
                assertThat(row.get("name")).isEqualTo("row-" + i);
            }
        }
    }

    private static byte[] writeWithAvroJava(String codecName) throws IOException {
        Schema schema = SchemaBuilder.record("Row")
                .fields()
                .requiredLong("id")
                .requiredString("name")
                .endRecord();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
            writer.setCodec(CodecFactory.fromString(codecName));
            writer.create(schema, out);
            for (int i = 0; i < ROWS; i++) {
                GenericRecord row = new GenericData.Record(schema);
                row.put("id", (long) i);
                row.put("name", "row-" + i);
                writer.append(row);
            }
        }
        return out.toByteArray();
    }
}
