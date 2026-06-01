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
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.data.read.BinaryValueSink;

class BinaryDecodeIntoTest {

    private static final String[] VALUES = {"alpha", "", "gamma", "delta"};

    @Test
    void plainBinaryAppendsValuesIntoSink() {
        assertDecodeIntoMatchesNext(BinaryDecodeIntoTest::loadPlainPage, VALUES);
    }

    @Test
    void deltaLengthAppendsValuesIntoSink() throws Exception {
        byte[] page = encodeDeltaLengthPage(VALUES);
        assertDecodeIntoMatchesNext(() -> loadDeltaLengthDecoder(page), VALUES);
    }

    /**
     * Drives {@code next()} on one decoder instance as the oracle, then {@code decodeBinaryInto} on a fresh decoder
     * over the same page, and verifies the sink reproduces every value's bytes exactly.
     */
    private static void assertDecodeIntoMatchesNext(
            Supplier<PageDecoder<MemorySegment>> decoderFactory, String[] expected) {
        byte[][] oracle = readEachValue(decoderFactory.get(), expected.length);

        BinaryValueSink sink = new BinaryValueSink();
        decoderFactory.get().decodeBinaryInto(expected.length, sink);

        assertThat(sink.count()).isEqualTo(expected.length);
        int[] offsets = sink.offsets();
        MemorySegment backing = sink.backing();
        for (int i = 0; i < expected.length; i++) {
            byte[] actual =
                    backing.asSlice(offsets[i], offsets[i + 1] - offsets[i]).toArray(JAVA_BYTE);
            assertThat(actual).as("value " + i).isEqualTo(oracle[i]);
        }
    }

    private static byte[][] readEachValue(PageDecoder<MemorySegment> decoder, int n) {
        byte[][] values = new byte[n][];
        for (int i = 0; i < n; i++) {
            values[i] = decoder.next().toArray(JAVA_BYTE);
        }
        return values;
    }

    private static PageDecoder<MemorySegment> loadPlainPage() {
        int totalSize = 0;
        for (String value : VALUES) {
            totalSize += Integer.BYTES + value.getBytes(StandardCharsets.UTF_8).length;
        }
        ByteBuffer page = ByteBuffer.allocate(totalSize).order(LITTLE_ENDIAN);
        for (String value : VALUES) {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            page.putInt(bytes.length);
            page.put(bytes);
        }
        page.flip();

        PlainBinaryDecoder decoder = new PlainBinaryDecoder();
        decoder.load(MemorySegment.ofBuffer(page), VALUES.length);
        return decoder;
    }

    private static PageDecoder<MemorySegment> loadDeltaLengthDecoder(byte[] page) {
        DeltaLengthByteArrayDecoder decoder = new DeltaLengthByteArrayDecoder();
        decoder.load(MemorySegment.ofArray(page), VALUES.length);
        return decoder;
    }

    private static byte[] encodeDeltaLengthPage(String[] values) throws Exception {
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
