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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.io.ByteRangeSource;

class AvroDataFileWriterTest {

    private static final String SCHEMA = "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
            + "{\"name\":\"id\",\"type\":\"long\"},"
            + "{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null}]}";

    private static final byte[] FIXED_SYNC = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

    @Test
    void writesRecordsReadableByTheReader() {
        InMemoryByteSink sink = new InMemoryByteSink();
        try (AvroDataFileWriter writer =
                AvroDataFileWriter.builder(SCHEMA).codec("deflate").build(sink)) {
            writer.write(Map.of("id", 1L, "name", "alice"));
            writer.write(Map.of("id", 2L)); // name defaults to null
        }
        ByteRangeSource back = new InMemoryByteRangeSource(sink.toByteArray());
        try (AvroDataFileReader reader = AvroDataFileReader.open(back)) {
            List<AvroRecord> rows = reader.records().toList();
            assertThat(rows).hasSize(2);
            assertThat(rows.get(0).get("id")).isEqualTo(1L);
            assertThat(rows.get(0).get("name")).isEqualTo("alice");
            assertThat(rows.get(1).get("name")).isNull();
            assertThat(reader.metadata().get("avro.schema")).isEqualTo(SCHEMA);
        }
    }

    @Test
    void injectedSyncMarkerProducesDeterministicOutput() {
        byte[] first = writeWithFixedSync();
        byte[] second = writeWithFixedSync();
        assertThat(first).isEqualTo(second);
    }

    @Test
    void rejectsNonPositiveBlockSize() {
        assertThatThrownBy(() -> AvroDataFileWriter.builder(SCHEMA).blockSize(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsSyncMarkerOfWrongLength() {
        byte[] fifteenBytes = new byte[15];
        assertThatThrownBy(() -> AvroDataFileWriter.builder(SCHEMA).syncMarker(fifteenBytes))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("16");
    }

    @Test
    void writesEveryRecordInAnIterable() {
        InMemoryByteSink sink = new InMemoryByteSink();
        try (AvroDataFileWriter writer = AvroDataFileWriter.builder(SCHEMA).build(sink)) {
            writer.write(List.of(Map.of("id", 1L, "name", "x"), Map.of("id", 2L, "name", "y")));
        }
        ByteRangeSource back = new InMemoryByteRangeSource(sink.toByteArray());
        try (AvroDataFileReader reader = AvroDataFileReader.open(back)) {
            List<AvroRecord> rows = reader.records().toList();
            assertThat(rows).hasSize(2);
            assertThat(rows.get(0).get("name")).isEqualTo("x");
            assertThat(rows.get(1).get("name")).isEqualTo("y");
        }
    }

    private static byte[] writeWithFixedSync() {
        InMemoryByteSink sink = new InMemoryByteSink();
        try (AvroDataFileWriter writer = AvroDataFileWriter.builder(SCHEMA)
                .codec("deflate")
                .syncMarker(FIXED_SYNC)
                .build(sink)) {
            writer.write(Map.of("id", 1L, "name", "alice"));
            writer.write(Map.of("id", 2L));
        }
        return sink.toByteArray();
    }
}
