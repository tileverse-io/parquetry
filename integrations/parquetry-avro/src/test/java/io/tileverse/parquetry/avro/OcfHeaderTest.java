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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.testkit.TestCorpus;

class OcfHeaderTest {

    private static final String SCHEMA =
            "{\"type\":\"record\",\"name\":\"r\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}";

    @Test
    void readsMetadataSchemaCodecAndSync() {
        byte[] ocf = new OcfTestWriter().schema(SCHEMA).codec("null").build();
        OcfHeader header = OcfHeader.read(new ByteRangeCursor(new InMemoryByteRangeSource(ocf)));

        assertThat(header.metadata()).containsKeys("avro.schema", "avro.codec");
        assertThat(header.codec()).isEqualTo(BlockCodec.NULL);
        assertThat(((AvroSchema.Record) header.schema()).field("id")).isPresent();
        assertThat(header.syncMarker()).hasSize(16);
        assertThat(header.firstBlockOffset()).isEqualTo(ocf.length);
    }

    @Test
    void defaultsToNullCodecWhenAbsent() {
        byte[] ocf = new OcfTestWriter().schema(SCHEMA).build();
        OcfHeader header = OcfHeader.read(new ByteRangeCursor(new InMemoryByteRangeSource(ocf)));
        assertThat(header.codec()).isEqualTo(BlockCodec.NULL);
    }

    @Test
    void rejectsBadMagic() {
        byte[] ocf = new OcfTestWriter().schema(SCHEMA).build();
        ocf[0] = 'X';
        ByteRangeCursor cursor = new ByteRangeCursor(new InMemoryByteRangeSource(ocf));
        assertThatThrownBy(() -> OcfHeader.read(cursor))
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("magic");
    }

    @Test
    void rejectsNegativeMetadataValueLength() {
        byte[] ocf = ocfWithHostileValueLength(-1);
        ByteRangeCursor cursor = new ByteRangeCursor(new InMemoryByteRangeSource(ocf));
        assertThatThrownBy(() -> OcfHeader.read(cursor))
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("out of range");
    }

    @Test
    void rejectsUnrepresentableMetadataBlockCount() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(new byte[] {'O', 'b', 'j', 1});
        OcfTestWriter.varLong(out, Long.MIN_VALUE);
        OcfTestWriter.varLong(out, 0); // the byte-size hint that follows a negative count
        ByteRangeCursor cursor = new ByteRangeCursor(new InMemoryByteRangeSource(out.toByteArray()));
        assertThatThrownBy(() -> OcfHeader.read(cursor))
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("not representable");
    }

    private static byte[] ocfWithHostileValueLength(long declaredValueLength) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(new byte[] {'O', 'b', 'j', 1});
        OcfTestWriter.varLong(out, 1);
        byte[] key = "avro.schema".getBytes(StandardCharsets.UTF_8);
        OcfTestWriter.varLong(out, key.length);
        out.writeBytes(key);
        OcfTestWriter.varLong(out, declaredValueLength);
        return out.toByteArray();
    }

    @Test
    void readsRealCorpusManifestHeader() throws Exception {
        Path manifest = TestCorpus.extractFile(
                "avro-reference/iceberg-manifest/manifest.avro", Files.createTempDirectory("ocf-header"));
        try (ByteRangeSource source = ByteRangeSource.ofFile(manifest)) {
            OcfHeader header = OcfHeader.read(new ByteRangeCursor(source));
            assertThat(header.codec()).isEqualTo(BlockCodec.NULL);
            assertThat(header.metadata()).containsKey("schema");
            assertThat(((AvroSchema.Record) header.schema()).field("data_file")).isPresent();
        }
    }
}
