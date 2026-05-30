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

class DeltaLengthByteArrayDecoderTest {

    @Test
    void decodesThreeStrings() throws Exception {
        String[] values = {"hi", "world", "parquetry"};
        byte[] page = encodeStringPage(values);

        PageDecoder<MemorySegment> decoder = new DeltaLengthByteArrayDecoder();
        decoder.load(MemorySegment.ofArray(page), values.length);

        for (String expected : values) {
            MemorySegment slice = decoder.next();
            byte[] buf = slice.toArray(JAVA_BYTE);
            assertThat(new String(buf, StandardCharsets.UTF_8)).isEqualTo(expected);
        }
    }

    @Test
    void skipAdvancesCursorCorrectly() throws Exception {
        String[] values = {"alpha", "beta", "gamma", "delta"};

        int[] lengths = new int[values.length];
        for (int i = 0; i < values.length; i++) lengths[i] = values[i].length();

        // blockSize=4 divides evenly; no padding needed
        byte[] lengthsBytes = DeltaBinaryPackedDecoderTest.encodeInts(lengths, 4, 2);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(lengthsBytes);
        for (String v : values) {
            out.write(v.getBytes(StandardCharsets.UTF_8));
        }

        PageDecoder<MemorySegment> decoder = new DeltaLengthByteArrayDecoder();
        decoder.load(MemorySegment.ofArray(out.toByteArray()), values.length);

        // Skip first two, then read the last two
        decoder.skip(2);
        for (int i = 2; i < values.length; i++) {
            MemorySegment slice = decoder.next();
            byte[] buf = slice.toArray(JAVA_BYTE);
            assertThat(new String(buf, StandardCharsets.UTF_8)).isEqualTo(values[i]);
        }
    }

    @Test
    void bulkDecodeBinaryFillsArray() throws Exception {
        String[] values = {"hi", "world", "parquetry"};
        byte[] page = encodeStringPage(values);

        PageDecoder<MemorySegment> decoder = new DeltaLengthByteArrayDecoder();
        decoder.load(MemorySegment.ofArray(page), values.length);

        MemorySegment[] dst = new MemorySegment[values.length];
        decoder.decodeBinary(values.length, dst, 0);

        for (int i = 0; i < values.length; i++) {
            byte[] buf = dst[i].toArray(JAVA_BYTE);
            assertThat(new String(buf, StandardCharsets.UTF_8)).as("value " + i).isEqualTo(values[i]);
        }
    }

    /**
     * Encodes a DELTA_LENGTH_BYTE_ARRAY page from the given strings. The length array is passed at its real length (no
     * block-boundary padding); the encoder then writes data only for the miniblocks that hold values, matching what
     * real Parquet writers emit.
     */
    private static byte[] encodeStringPage(String[] values) throws Exception {
        int[] lengths = new int[values.length];
        for (int i = 0; i < values.length; i++) {
            lengths[i] = values[i].getBytes(StandardCharsets.UTF_8).length;
        }
        byte[] lengthsBytes = DeltaBinaryPackedDecoderTest.encodeInts(lengths, 4, 2);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(lengthsBytes);
        for (String v : values) {
            out.write(v.getBytes(StandardCharsets.UTF_8));
        }
        return out.toByteArray();
    }
}
