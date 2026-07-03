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
package io.tileverse.parquetry.internal.write.page;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class BinaryDictionaryEncoderTest {

    private static final long LIMIT = 256;

    static Stream<Arguments> sequences() {
        List<byte[]> lowCardinality = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            lowCardinality.add(("value-" + (i % 5)).getBytes(StandardCharsets.UTF_8));
        }
        List<byte[]> overflowing = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            overflowing.add(("distinct-value-" + i).getBytes(StandardCharsets.UTF_8));
        }
        List<byte[]> withEmptyAndPrefixes = List.of(
                new byte[0],
                "a".getBytes(StandardCharsets.UTF_8),
                "ab".getBytes(StandardCharsets.UTF_8),
                "a".getBytes(StandardCharsets.UTF_8),
                new byte[0],
                "abc".getBytes(StandardCharsets.UTF_8));
        Random random = new Random(42);
        List<byte[]> randomMix = new ArrayList<>();
        for (int i = 0; i < 5000; i++) {
            byte[] bytes = new byte[random.nextInt(12)];
            random.nextBytes(bytes);
            randomMix.add(bytes);
        }
        return Stream.of(
                Arguments.of("lowCardinality", lowCardinality, 64),
                Arguments.of("overflowing", overflowing, 50),
                Arguments.of("emptyAndPrefixes", withEmptyAndPrefixes, 3),
                Arguments.of("randomMix", randomMix, 500));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("sequences")
    void matchesGenericEncoderPageByPage(String name, List<byte[]> values, int pageSize) throws IOException {
        DictionaryAttemptEncoder<ByteBuffer, byte[][]> reference = referenceVariableLengthEncoder(LIMIT);
        BinaryDictionaryEncoder tested = BinaryDictionaryEncoder.variableLength(new PlainBinaryEncoder(), LIMIT);

        for (int i = 0; i < values.size(); i++) {
            byte[] value = values.get(i);
            reference.appendValue(ByteBuffer.wrap(value));
            tested.appendValue(MemorySegment.ofArray(value), value);
            boolean pageBoundary = (i + 1) % pageSize == 0 || i == values.size() - 1;
            if (pageBoundary) {
                assertPageFlushIdentical(reference, tested, name + " page ending at " + i);
            }
        }
        assertThat(tested.overflowed()).isEqualTo(reference.overflowed());
        assertThat(tested.emittedDictionaryPage()).isEqualTo(reference.emittedDictionaryPage());
        assertThat(tested.dictionaryCarrier()).isEqualTo(carrierOf(reference.dictionaryValues()));
    }

    @Test
    void fixedLengthSizingMatchesGenericEncoder() throws IOException {
        int len = 12;
        DictionaryAttemptEncoder<ByteBuffer, byte[][]> reference = referenceFixedLengthEncoder(LIMIT, len);
        BinaryDictionaryEncoder tested =
                BinaryDictionaryEncoder.fixedLength(new PlainFixedLenBinaryEncoder(len), len, LIMIT);
        Random random = new Random(7);
        for (int i = 0; i < 100; i++) {
            byte[] value = new byte[len];
            random.nextBytes(value);
            if (i % 3 == 0) {
                random.setSeed(7 + i % 5); // reuse a handful of values to exercise the dedupe path
                random.nextBytes(value);
            }
            reference.appendValue(ByteBuffer.wrap(value));
            tested.appendValue(MemorySegment.ofArray(value), value);
        }
        assertPageFlushIdentical(reference, tested, "fixed-length page");
        assertThat(tested.overflowed()).isEqualTo(reference.overflowed());
        assertThat(tested.dictionaryCarrier()).isEqualTo(carrierOf(reference.dictionaryValues()));
    }

    @Test
    void byteArrayConvenienceMatchesSegmentPath() throws IOException {
        BinaryDictionaryEncoder viaSegment = BinaryDictionaryEncoder.variableLength(new PlainBinaryEncoder(), LIMIT);
        BinaryDictionaryEncoder viaArray = BinaryDictionaryEncoder.variableLength(new PlainBinaryEncoder(), LIMIT);
        byte[][] values = {
            "one".getBytes(StandardCharsets.UTF_8),
            "two".getBytes(StandardCharsets.UTF_8),
            "one".getBytes(StandardCharsets.UTF_8)
        };
        for (byte[] value : values) {
            viaSegment.appendValue(MemorySegment.ofArray(value), value);
            viaArray.appendValue(value);
        }
        assertThat(flushToBytes(viaArray)).isEqualTo(flushToBytes(viaSegment));
        assertThat(viaArray.dictionaryCarrier()).isEqualTo(viaSegment.dictionaryCarrier());
    }

    @Test
    void tableGrowthKeepsDedupeAndIndexOrder() throws IOException {
        DictionaryAttemptEncoder<ByteBuffer, byte[][]> reference = referenceVariableLengthEncoder(Long.MAX_VALUE);
        BinaryDictionaryEncoder tested =
                BinaryDictionaryEncoder.variableLength(new PlainBinaryEncoder(), Long.MAX_VALUE);
        Random random = new Random(1234);
        for (int i = 0; i < 10_000; i++) {
            // ~2000 distinct values force several grow-and-rehash rounds; repeats exercise post-growth lookups.
            byte[] value = ("key-" + random.nextInt(2000)).getBytes(StandardCharsets.UTF_8);
            reference.appendValue(ByteBuffer.wrap(value));
            tested.appendValue(MemorySegment.ofArray(value), value);
            if ((i + 1) % 1000 == 0) {
                assertPageFlushIdentical(reference, tested, "growth page ending at " + i);
            }
        }
        assertThat(tested.overflowed()).isFalse();
        assertThat(tested.dictionaryCarrier()).isEqualTo(carrierOf(reference.dictionaryValues()));
    }

    private static void assertPageFlushIdentical(
            DictionaryAttemptEncoder<ByteBuffer, byte[][]> reference, BinaryDictionaryEncoder tested, String context)
            throws IOException {
        ByteArrayOutputStream referenceBytes = new ByteArrayOutputStream();
        PageDictionaryEncoder.PageResult referenceResult = reference.flushPage(Channels.newChannel(referenceBytes));
        ByteArrayOutputStream testedBytes = new ByteArrayOutputStream();
        PageDictionaryEncoder.PageResult testedResult = tested.flushPage(Channels.newChannel(testedBytes));
        assertThat(testedBytes.toByteArray()).as(context).isEqualTo(referenceBytes.toByteArray());
        assertThat(testedResult).as(context).isEqualTo(referenceResult);
    }

    private static byte[] flushToBytes(BinaryDictionaryEncoder encoder) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        encoder.flushPage(Channels.newChannel(out));
        return out.toByteArray();
    }

    /** Constructed exactly as DictionaryAttempt.BinaryAttempt.create wires the generic encoder today. */
    private static DictionaryAttemptEncoder<ByteBuffer, byte[][]> referenceVariableLengthEncoder(long limit) {
        return new DictionaryAttemptEncoder<>(
                new PlainBinaryEncoder(),
                BinaryDictionaryEncoderTest::carrierOf,
                value -> (long) Integer.BYTES + value.remaining(),
                limit);
    }

    private static DictionaryAttemptEncoder<ByteBuffer, byte[][]> referenceFixedLengthEncoder(long limit, int length) {
        return new DictionaryAttemptEncoder<>(
                new PlainFixedLenBinaryEncoder(length),
                BinaryDictionaryEncoderTest::carrierOf,
                value -> (long) length,
                limit);
    }

    private static byte[][] carrierOf(List<ByteBuffer> values) {
        byte[][] out = new byte[values.size()][];
        for (int i = 0; i < values.size(); i++) {
            ByteBuffer view = values.get(i).duplicate();
            byte[] copy = new byte[view.remaining()];
            view.get(copy);
            out[i] = copy;
        }
        return out;
    }
}
