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
package io.tileverse.parquetry.data.read.page;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

class DeltaByteArrayDecoderTest {

    @Test
    void decodesPrefixSharedStrings() throws Exception {
        byte[] page = encodePrefixSharedPage();

        PageDecoder<MemorySegment> decoder = new DeltaByteArrayDecoder();
        decoder.load(MemorySegment.ofArray(page), 3);

        for (String expected : new String[] {"car", "carpet", "carry"}) {
            MemorySegment slice = decoder.next();
            byte[] buf = slice.toArray(JAVA_BYTE);
            assertThat(new String(buf, StandardCharsets.UTF_8)).isEqualTo(expected);
        }
    }

    @Test
    void firstValueHasZeroPrefix() throws Exception {
        // Single-value page: the only value has prefix 0 and the whole string as its suffix.
        int blockSize = 4;
        int miniblocksPerBlock = 2;
        byte[] prefixBytes = DeltaBinaryPackedDecoderTest.encodeInts(new int[] {0}, blockSize, miniblocksPerBlock);
        byte[] suffixLenBytes = DeltaBinaryPackedDecoderTest.encodeInts(new int[] {5}, blockSize, miniblocksPerBlock);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(prefixBytes);
        out.write(suffixLenBytes);
        out.write("hello".getBytes(StandardCharsets.UTF_8));

        PageDecoder<MemorySegment> decoder = new DeltaByteArrayDecoder();
        decoder.load(MemorySegment.ofArray(out.toByteArray()), 1);

        MemorySegment slice = decoder.next();
        byte[] buf = slice.toArray(JAVA_BYTE);
        assertThat(new String(buf, StandardCharsets.UTF_8)).isEqualTo("hello");
    }

    @Test
    void bulkDecodeBinaryFillsArray() throws Exception {
        byte[] page = encodePrefixSharedPage();

        PageDecoder<MemorySegment> decoder = new DeltaByteArrayDecoder();
        decoder.load(MemorySegment.ofArray(page), 3);

        MemorySegment[] dst = new MemorySegment[3];
        decoder.decodeBinary(3, dst, 0);

        String[] values = {"car", "carpet", "carry"};
        for (int i = 0; i < values.length; i++) {
            byte[] buf = dst[i].toArray(JAVA_BYTE);
            assertThat(new String(buf, StandardCharsets.UTF_8)).as("value " + i).isEqualTo(values[i]);
        }
    }

    /**
     * Encodes the classic ["car", "carpet", "carry"] DELTA_BYTE_ARRAY page. The prefix and suffix length arrays are
     * passed at their real length (no block-boundary padding); the encoder then writes data only for the miniblocks
     * that hold values, matching what real Parquet writers emit.
     */
    private static byte[] encodePrefixSharedPage() throws Exception {
        int blockSize = 4;
        int miniblocksPerBlock = 2;
        int[] prefixLens = {0, 3, 3};
        int[] suffixLens = {3, 3, 2};
        String[] suffixes = {"car", "pet", "ry"};

        byte[] prefixBytes = DeltaBinaryPackedDecoderTest.encodeInts(prefixLens, blockSize, miniblocksPerBlock);
        byte[] suffixLenBytes = DeltaBinaryPackedDecoderTest.encodeInts(suffixLens, blockSize, miniblocksPerBlock);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(prefixBytes);
        out.write(suffixLenBytes);
        for (String suffix : suffixes) {
            out.write(suffix.getBytes(StandardCharsets.UTF_8));
        }
        return out.toByteArray();
    }
}
