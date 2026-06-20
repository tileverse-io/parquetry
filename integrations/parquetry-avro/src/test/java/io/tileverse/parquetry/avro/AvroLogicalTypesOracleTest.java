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
package io.tileverse.parquetry.avro;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

/**
 * Writes one OCF with avro-java holding every interpreted logical type as its raw underlying value, then reads it back
 * with {@link AvroDataFileReader} and asserts the typed values this module produces.
 */
class AvroLogicalTypesOracleTest {

    private static final String SCHEMA_JSON = """
            {"type":"record","name":"Logicals","fields":[
              {"name":"dec","type":{"type":"bytes","logicalType":"decimal","precision":9,"scale":2}},
              {"name":"id","type":{"type":"string","logicalType":"uuid"}},
              {"name":"day","type":{"type":"int","logicalType":"date"}},
              {"name":"tod_millis","type":{"type":"int","logicalType":"time-millis"}},
              {"name":"tod_micros","type":{"type":"long","logicalType":"time-micros"}},
              {"name":"ts_millis","type":{"type":"long","logicalType":"timestamp-millis"}},
              {"name":"ts_micros","type":{"type":"long","logicalType":"timestamp-micros"}},
              {"name":"local_ts_millis","type":{"type":"long","logicalType":"local-timestamp-millis"}},
              {"name":"local_ts_micros","type":{"type":"long","logicalType":"local-timestamp-micros"}},
              {"name":"dur","type":{"type":"fixed","name":"dur12","size":12,"logicalType":"duration"}}]}
            """;

    private static final String UUID_TEXT = "f81d4fae-7dec-11d0-a765-00a0c91e6bf6";

    @Test
    void readsLogicalValuesFromAvroJavaWrittenFile() throws IOException {
        byte[] file = writeWithAvroJava();

        try (AvroDataFileReader reader = AvroDataFileReader.open(new InMemoryByteRangeSource(file))) {
            List<AvroRecord> records = reader.records().toList();
            assertThat(records).hasSize(1);

            AvroRecord row = records.get(0);
            assertThat(row.get("dec")).isEqualTo(new BigDecimal("123.45"));
            assertThat(row.get("id")).isEqualTo(UUID.fromString(UUID_TEXT));
            assertThat(row.get("day")).isEqualTo(LocalDate.of(2022, 1, 8));
            assertThat(row.get("tod_millis")).isEqualTo(LocalTime.of(1, 1, 1, 1_000_000));
            assertThat(row.get("tod_micros")).isEqualTo(LocalTime.of(1, 1, 1, 1_000));
            assertThat(row.get("ts_millis")).isEqualTo(Instant.parse("1970-01-01T00:00:01Z"));
            assertThat(row.get("ts_micros")).isEqualTo(Instant.parse("1969-12-31T23:59:59.999999Z"));
            assertThat(row.get("local_ts_millis")).isEqualTo(LocalDateTime.of(1970, 1, 2, 0, 0));
            assertThat(row.get("local_ts_micros")).isEqualTo(LocalDateTime.of(1970, 1, 1, 0, 0, 1, 1_000));
            assertThat(row.get("dur")).isEqualTo(new AvroDuration(1, 2, 3));
        }
    }

    private static byte[] writeWithAvroJava() throws IOException {
        Schema schema = new Schema.Parser().parse(SCHEMA_JSON);
        Schema durationSchema = schema.getField("dur").schema();

        GenericRecord row = new GenericData.Record(schema);
        row.put("dec", ByteBuffer.wrap(new byte[] {0x30, 0x39}));
        row.put("id", UUID_TEXT);
        row.put("day", 19000);
        row.put("tod_millis", 3_661_001);
        row.put("tod_micros", 3_661_000_001L);
        row.put("ts_millis", 1_000L);
        row.put("ts_micros", -1L);
        row.put("local_ts_millis", 86_400_000L);
        row.put("local_ts_micros", 1_000_001L);
        row.put("dur", new GenericData.Fixed(durationSchema, littleEndianDuration(1, 2, 3)));

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
            writer.create(schema, out);
            writer.append(row);
        }
        return out.toByteArray();
    }

    private static byte[] littleEndianDuration(int months, int days, int millis) {
        ByteBuffer buffer = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(months).putInt(days).putInt(millis);
        return buffer.array();
    }
}
