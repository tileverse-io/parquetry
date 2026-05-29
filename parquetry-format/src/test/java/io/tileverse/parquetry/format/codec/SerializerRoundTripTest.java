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
package io.tileverse.parquetry.format.codec;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.BloomFilterHeader;

/**
 * Direct, package-private round-trip coverage for serializers that aren't reachable through the public
 * {@link io.tileverse.parquetry.format.ParquetFormat} surface. Currently only {@link BloomFilterHeader}, since it has
 * no top-level write entrypoint.
 */
class SerializerRoundTripTest {

    @Test
    void bloomFilterHeaderRoundTripsAllVariants() throws IOException {
        BloomFilterHeader original = new BloomFilterHeader(
                1024,
                BloomFilterHeader.Algorithm.SPLIT_BLOCK,
                BloomFilterHeader.HashStrategy.XXHASH,
                BloomFilterHeader.Compression.UNCOMPRESSED);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BloomFilterHeaderSerializer.serialize(new CompactProtocolWriter(out), original);
        BloomFilterHeader parsed =
                ParquetFormatDeserializer.readBloomFilterHeader(new ByteArrayInputStream(out.toByteArray()));
        assertThat(parsed).isEqualTo(original);
    }
}
