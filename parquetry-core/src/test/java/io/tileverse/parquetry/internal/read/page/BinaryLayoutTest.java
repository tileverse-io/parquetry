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
package io.tileverse.parquetry.internal.read.page;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

class BinaryLayoutTest {

    private static final String[] VALUES = {"alpha", "", "gamma", "delta"};

    @Test
    void plainLayoutPositionsAndLengthsMatchNext() {
        MemorySegment page = plainPage(VALUES);
        assertLayoutMatchesNext(page, p -> new PlainBinaryDecoder(), VALUES);
    }

    @Test
    void deltaLengthLayoutPositionsAndLengthsMatchNext() throws Exception {
        MemorySegment page = MemorySegment.ofArray(deltaLengthPage(VALUES));
        assertLayoutMatchesNext(page, p -> new DeltaLengthByteArrayDecoder(), VALUES);
    }

    private static void assertLayoutMatchesNext(
            MemorySegment page, Function<MemorySegment, PageDecoder<MemorySegment>> decoderFactory, String[] expected) {
        byte[][] oracle = readEachValue(loaded(page, decoderFactory), expected.length);

        int[] positions = new int[expected.length];
        int[] lengths = new int[expected.length];
        loaded(page, decoderFactory).decodeBinaryLayout(expected.length, positions, lengths, 0);

        for (int i = 0; i < expected.length; i++) {
            byte[] actual = page.asSlice(positions[i], lengths[i]).toArray(JAVA_BYTE);
            assertThat(actual).as("value %d bytes", i).isEqualTo(oracle[i]);
        }
    }

    private static PageDecoder<MemorySegment> loaded(
            MemorySegment page, Function<MemorySegment, PageDecoder<MemorySegment>> decoderFactory) {
        PageDecoder<MemorySegment> decoder = decoderFactory.apply(page);
        decoder.load(page, VALUES.length);
        return decoder;
    }

    private static byte[][] readEachValue(PageDecoder<MemorySegment> decoder, int n) {
        byte[][] values = new byte[n][];
        for (int i = 0; i < n; i++) {
            values[i] = decoder.next().toArray(JAVA_BYTE);
        }
        return values;
    }

    private static MemorySegment plainPage(String[] values) {
        int totalSize = 0;
        for (String value : values) {
            totalSize += Integer.BYTES + value.getBytes(StandardCharsets.UTF_8).length;
        }
        ByteBuffer page = ByteBuffer.allocate(totalSize).order(LITTLE_ENDIAN);
        for (String value : values) {
            byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
            page.putInt(valueBytes.length);
            page.put(valueBytes);
        }
        page.flip();
        return MemorySegment.ofBuffer(page);
    }

    private static byte[] deltaLengthPage(String[] values) throws Exception {
        int[] lengths = new int[values.length];
        for (int i = 0; i < values.length; i++) {
            lengths[i] = values[i].getBytes(StandardCharsets.UTF_8).length;
        }
        byte[] lengthsBytes = DeltaBinaryPackedDecoderTest.encodeInts(lengths, 4, 2);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(lengthsBytes);
        for (String value : values) {
            out.write(value.getBytes(StandardCharsets.UTF_8));
        }
        return out.toByteArray();
    }
}
