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
package io.tileverse.parquetry.page.delta;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.page.PageDecoder;

class DeltaByteArrayDecoderTest {

    @Test
    void decodesPrefixSharedStrings() throws Exception {
        // Classic example: ["car", "carpet", "carry"]
        String[] values = {"car", "carpet", "carry"};
        int[] prefixLens = {0, 3, 3};
        String[] suffixes = {"car", "pet", "ry"};

        // Both prefix-lengths and suffix-lengths are DELTA_BINARY_PACKED. Block size needs
        // to be >= padded array size. Use block=4 / miniblocks=2 with 3 actual values.
        int blockSize = 4;
        int miniblocksPerBlock = 2;

        int[] paddedPrefix = new int[blockSize];
        System.arraycopy(prefixLens, 0, paddedPrefix, 0, prefixLens.length);
        for (int i = prefixLens.length; i < paddedPrefix.length; i++) {
            paddedPrefix[i] = prefixLens[prefixLens.length - 1];
        }

        int[] suffixLens = new int[blockSize];
        for (int i = 0; i < suffixes.length; i++) {
            suffixLens[i] = suffixes[i].length();
        }
        for (int i = suffixes.length; i < suffixLens.length; i++) {
            suffixLens[i] = suffixes[suffixes.length - 1].length();
        }

        byte[] prefixBytes =
                DeltaBinaryPackedDecoderTest.encodeInts(paddedPrefix, blockSize, miniblocksPerBlock, values.length);
        byte[] suffixLenBytes =
                DeltaBinaryPackedDecoderTest.encodeInts(suffixLens, blockSize, miniblocksPerBlock, values.length);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(prefixBytes);
        out.write(suffixLenBytes);
        for (String suffix : suffixes) {
            out.write(suffix.getBytes(StandardCharsets.UTF_8));
        }
        byte[] page = out.toByteArray();

        PageDecoder<ByteBuffer> decoder = new DeltaByteArrayDecoder();
        decoder.load(ByteBuffer.wrap(page), values.length);

        for (String expected : values) {
            ByteBuffer slice = decoder.next();
            byte[] buf = new byte[slice.remaining()];
            slice.get(buf);
            assertThat(new String(buf, StandardCharsets.UTF_8)).isEqualTo(expected);
        }
    }

    @Test
    void firstValueHasZeroPrefix() throws Exception {
        // Edge case: single-value page with prefix 0
        String[] values = {"hello"};
        int blockSize = 4;
        int miniblocksPerBlock = 2;

        int[] prefixLens = new int[blockSize]; // all zeros (padding == 0)
        int[] suffixLens = new int[blockSize];
        suffixLens[0] = 5;
        // pad with the last actual value (5)
        for (int i = 1; i < suffixLens.length; i++) suffixLens[i] = 5;

        byte[] prefixBytes =
                DeltaBinaryPackedDecoderTest.encodeInts(prefixLens, blockSize, miniblocksPerBlock, values.length);
        byte[] suffixLenBytes =
                DeltaBinaryPackedDecoderTest.encodeInts(suffixLens, blockSize, miniblocksPerBlock, values.length);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(prefixBytes);
        out.write(suffixLenBytes);
        out.write("hello".getBytes(StandardCharsets.UTF_8));

        PageDecoder<ByteBuffer> decoder = new DeltaByteArrayDecoder();
        decoder.load(ByteBuffer.wrap(out.toByteArray()), 1);

        ByteBuffer slice = decoder.next();
        byte[] buf = new byte[slice.remaining()];
        slice.get(buf);
        assertThat(new String(buf, StandardCharsets.UTF_8)).isEqualTo("hello");
    }
}
