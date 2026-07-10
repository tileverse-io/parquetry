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

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.io.ByteRangeSource;

class OcfWriterTest {

    private static final String SCHEMA =
            "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"a\",\"type\":\"int\"}]}";

    private static final byte[] SYNC = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};

    @Test
    void writesHeaderAndBlocksReadableByTheReader() {
        InMemoryByteSink sink = new InMemoryByteSink();
        AvroSchema schema = AvroSchema.parse(SCHEMA);
        AvroDatumEncoder datumEncoder = new AvroDatumEncoder();
        try (OcfWriter writer = new OcfWriter(sink, SCHEMA, "null", SYNC, 1)) {
            for (int value : new int[] {10, 20, 30}) {
                AvroBinaryEncoder record = new AvroBinaryEncoder();
                datumEncoder.encode(schema, Map.of("a", value), record);
                writer.append(record.toByteArray());
            }
        }

        assertThat(readBackValues(sink.toByteArray())).containsExactly(10, 20, 30);
    }

    @Test
    void deflateCodecWithSmallThresholdForcesMultipleBlocks() {
        InMemoryByteSink sink = new InMemoryByteSink();
        AvroSchema schema = AvroSchema.parse(SCHEMA);
        AvroDatumEncoder datumEncoder = new AvroDatumEncoder();
        try (OcfWriter writer = new OcfWriter(sink, SCHEMA, "deflate", SYNC, 1)) {
            for (int value : new int[] {1, 2, 3, 4, 5}) {
                AvroBinaryEncoder record = new AvroBinaryEncoder();
                datumEncoder.encode(schema, Map.of("a", value), record);
                writer.append(record.toByteArray());
            }
        }

        assertThat(readBackValues(sink.toByteArray())).containsExactly(1, 2, 3, 4, 5);
    }

    private static List<Integer> readBackValues(byte[] bytes) {
        ByteRangeSource back = new InMemoryByteRangeSource(bytes);
        try (AvroDataFileReader reader = AvroDataFileReader.open(back)) {
            return reader.records().map(record -> (Integer) record.get("a")).toList();
        }
    }
}
