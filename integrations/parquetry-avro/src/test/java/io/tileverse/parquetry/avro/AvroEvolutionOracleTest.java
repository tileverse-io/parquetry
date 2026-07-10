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
package io.tileverse.parquetry.avro;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

/**
 * Writes a file with avro-java under a V1 schema and reads it under an evolved V2 reader schema with both avro-java
 * (the oracle) and this module's {@link AvroDataFileReader#records(AvroSchema)}, asserting the resolved records agree
 * field by field. The evolution exercises projection (tags dropped), a field alias (name to title), an int-to-long
 * promotion (rank), a defaulted reader-only field (score), an enum default fallback (BLUE to RED), and writer-union
 * resolution (note, nullable in both versions, alternating null rows).
 */
class AvroEvolutionOracleTest {

    private static final int ROWS = 200;

    private static final String WRITER_SCHEMA_JSON = """
            {"type":"record","name":"Row","fields":[
              {"name":"id","type":"long"},
              {"name":"rank","type":"int"},
              {"name":"name","type":"string"},
              {"name":"tags","type":{"type":"array","items":"string"}},
              {"name":"note","type":["null","string"],"default":null},
              {"name":"color","type":{"type":"enum","name":"Color","symbols":["RED","GREEN","BLUE"],"default":"RED"}}
            ]}""";

    private static final String READER_SCHEMA_JSON = """
            {"type":"record","name":"Row","fields":[
              {"name":"id","type":"long"},
              {"name":"title","type":"string","aliases":["name"]},
              {"name":"rank","type":"long"},
              {"name":"score","type":"double","default":1.5},
              {"name":"note","type":["null","string"],"default":null},
              {"name":"color","type":{"type":"enum","name":"Color","symbols":["RED","GREEN"],"default":"RED"}}
            ]}""";

    @Test
    void matchesAvroJavaSchemaResolution() throws IOException {
        byte[] file = writeWithAvroJava();

        List<GenericRecord> expected = readWithAvroJava(file);
        List<AvroRecord> actual = readWithOurReader(file);

        assertThat(expected).hasSize(ROWS);
        assertThat(actual).hasSize(ROWS);
        for (int i = 0; i < ROWS; i++) {
            Object expectedRecord = OracleValueNormalizer.normalize(expected.get(i));
            Object actualRecord = OracleValueNormalizer.normalize(actual.get(i));
            assertThat(actualRecord).as("record %d", i).isEqualTo(expectedRecord);
        }
    }

    private static byte[] writeWithAvroJava() throws IOException {
        Schema schema = new Schema.Parser().parse(WRITER_SCHEMA_JSON);
        Schema colorSchema = schema.getField("color").schema();
        List<String> colors = colorSchema.getEnumSymbols();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
            writer.create(schema, out);
            for (int i = 0; i < ROWS; i++) {
                GenericRecord row = new GenericData.Record(schema);
                row.put("id", (long) i);
                row.put("rank", i % 47);
                row.put("name", "row-" + i);
                row.put("tags", tags(i));
                row.put("note", i % 2 == 0 ? null : "note-" + i);
                row.put("color", new GenericData.EnumSymbol(colorSchema, colors.get(i % colors.size())));
                writer.append(row);
            }
        }
        return out.toByteArray();
    }

    private static List<String> tags(int row) {
        List<String> tags = new ArrayList<>();
        for (int i = 0; i < row % 4; i++) {
            tags.add("tag-" + row + "-" + i);
        }
        return tags;
    }

    private static List<GenericRecord> readWithAvroJava(byte[] file) throws IOException {
        Schema writerSchema = new Schema.Parser().parse(WRITER_SCHEMA_JSON);
        Schema readerSchema = new Schema.Parser().parse(READER_SCHEMA_JSON);
        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(writerSchema, readerSchema);
        List<GenericRecord> records = new ArrayList<>();
        try (DataFileReader<GenericRecord> reader =
                new DataFileReader<>(new SeekableByteArrayInput(file), datumReader)) {
            for (GenericRecord row : reader) {
                records.add(row);
            }
        }
        return records;
    }

    private static List<AvroRecord> readWithOurReader(byte[] file) {
        AvroSchema readerSchema = AvroSchema.parse(READER_SCHEMA_JSON);
        try (AvroDataFileReader reader = AvroDataFileReader.open(new InMemoryByteRangeSource(file))) {
            return reader.records(readerSchema).toList();
        }
    }
}
