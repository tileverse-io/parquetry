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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.format.MalformedFileException;

class PlainBinaryDecoderTest {

    @Test
    void decodesVariableLengthEntries() {
        byte[] empty = new byte[0];
        byte[] hi = "hi".getBytes(StandardCharsets.UTF_8);
        byte[] world = "world!".getBytes(StandardCharsets.UTF_8);

        // Total: 3 entries * 4-byte prefix + 0+2+6 payload = 12 + 8 = 20 bytes
        int totalSize = 3 * 4 + empty.length + hi.length + world.length;
        ByteBuffer page = ByteBuffer.allocate(totalSize).order(LITTLE_ENDIAN);
        page.putInt(empty.length);
        page.put(empty);
        page.putInt(hi.length);
        page.put(hi);
        page.putInt(world.length);
        page.put(world);
        page.flip();

        PageDecoder<MemorySegment> decoder = new PlainBinaryDecoder();
        decoder.load(MemorySegment.ofBuffer(page), 3);

        MemorySegment resultEmpty = decoder.next();
        assertThat(resultEmpty.byteSize()).isZero();

        MemorySegment resultHi = decoder.next();
        assertThat(resultHi.byteSize()).isEqualTo(2);
        assertThat(resultHi.toArray(JAVA_BYTE)).isEqualTo(hi);

        MemorySegment resultWorld = decoder.next();
        assertThat(resultWorld.byteSize()).isEqualTo(6);
        assertThat(resultWorld.toArray(JAVA_BYTE)).isEqualTo(world);
    }

    @Test
    void slicesAreReadOnly() {
        ByteBuffer page = ByteBuffer.allocate(4).order(LITTLE_ENDIAN);
        page.putInt(0); // empty entry
        page.flip();

        PageDecoder<MemorySegment> decoder = new PlainBinaryDecoder();
        decoder.load(MemorySegment.ofBuffer(page), 1);

        MemorySegment slice = decoder.next();
        assertThat(slice.isReadOnly()).isTrue();
    }

    @Test
    void skipJumpsOverLengthPrefixedEntries() {
        byte[] skipped1 = "skip1".getBytes(StandardCharsets.UTF_8);
        byte[] skipped2 = "skip2!".getBytes(StandardCharsets.UTF_8);
        byte[] kept = "kept".getBytes(StandardCharsets.UTF_8);

        int totalSize = 3 * 4 + skipped1.length + skipped2.length + kept.length;
        ByteBuffer page = ByteBuffer.allocate(totalSize).order(LITTLE_ENDIAN);
        page.putInt(skipped1.length);
        page.put(skipped1);
        page.putInt(skipped2.length);
        page.put(skipped2);
        page.putInt(kept.length);
        page.put(kept);
        page.flip();

        PageDecoder<MemorySegment> decoder = new PlainBinaryDecoder();
        decoder.load(MemorySegment.ofBuffer(page), 3);
        decoder.skip(2);

        MemorySegment result = decoder.next();
        assertThat(result.toArray(JAVA_BYTE)).isEqualTo(kept);
    }

    @Test
    void bulkDecodeFillsArrayOfSegments() {
        ByteBuffer page = ByteBuffer.allocate(64).order(LITTLE_ENDIAN);
        page.putInt(3);
        page.put("foo".getBytes());
        page.putInt(2);
        page.put("hi".getBytes());
        page.putInt(5);
        page.put("hello".getBytes());
        page.flip();

        PageDecoder<MemorySegment> decoder = new PlainBinaryDecoder();
        decoder.load(MemorySegment.ofBuffer(page), 3);

        MemorySegment[] dst = new MemorySegment[3];
        decoder.decodeBinary(3, dst, 0);

        assertThat(dst[0].toArray(JAVA_BYTE)).isEqualTo("foo".getBytes());
        assertThat(dst[1].toArray(JAVA_BYTE)).isEqualTo("hi".getBytes());
        assertThat(dst[2].toArray(JAVA_BYTE)).isEqualTo("hello".getBytes());
    }

    static Stream<Arguments> malformedBinaryPages() {
        // each vector is a page, the values decoded from it, and the expected failure
        return Stream.of(
                // a length prefix cut short after two of its four bytes
                Arguments.of(new byte[] {2, 0}, 1, "values end after 2 bytes"),
                // a value announcing 5 bytes with 2 left in the page
                Arguments.of(new byte[] {5, 0, 0, 0, 'a', 'b'}, 1, "declares a length of 5 but 2 bytes remain"),
                // a negative length prefix
                Arguments.of(
                        new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff}, 1, "declares a length of -1"),
                // one complete value, then a second value requested from an exhausted page
                Arguments.of(new byte[] {1, 0, 0, 0, 'a'}, 2, "values end after 5 bytes"));
    }

    @ParameterizedTest
    @MethodSource("malformedBinaryPages")
    void malformedPageFailsEachDecodeLaneWithAFormatError(byte[] page, int valueCount, String failure) {
        assertThatThrownBy(() -> decodeValues(page, valueCount))
                .isInstanceOf(MalformedFileException.class)
                .hasMessageContaining(failure);
        assertThatThrownBy(() -> decodeLayout(page, valueCount))
                .isInstanceOf(MalformedFileException.class)
                .hasMessageContaining(failure);
        assertThatThrownBy(() -> skipValues(page, valueCount))
                .isInstanceOf(MalformedFileException.class)
                .hasMessageContaining(failure);
    }

    private static void decodeValues(byte[] page, int valueCount) {
        PlainBinaryDecoder decoder = new PlainBinaryDecoder();
        decoder.load(MemorySegment.ofArray(page), valueCount);
        decoder.decodeBinary(valueCount, new MemorySegment[valueCount], 0);
    }

    private static void decodeLayout(byte[] page, int valueCount) {
        PlainBinaryDecoder decoder = new PlainBinaryDecoder();
        decoder.load(MemorySegment.ofArray(page), valueCount);
        decoder.decodeBinaryLayout(valueCount, new int[valueCount], new int[valueCount], 0);
    }

    private static void skipValues(byte[] page, int valueCount) {
        PlainBinaryDecoder decoder = new PlainBinaryDecoder();
        decoder.load(MemorySegment.ofArray(page), valueCount);
        decoder.skip(valueCount);
    }
}
