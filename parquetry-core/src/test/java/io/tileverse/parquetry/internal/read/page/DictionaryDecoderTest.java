/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.schema.PrimitiveKind;

class DictionaryDecoderTest {

    @Test
    void readIntDictionary() {
        // Three INT32 values: 100, 200, 300 in little-endian
        ByteBuffer page = ByteBuffer.allocate(12).order(LITTLE_ENDIAN);
        page.putInt(100);
        page.putInt(200);
        page.putInt(300);
        page.flip();

        Dictionary<?> dict =
                DictionaryDecoder.read(MemorySegment.ofBuffer(page), PrimitiveKind.INT32, 3, OptionalInt.empty());

        assertThat(dict).isInstanceOf(Dictionary.IntDict.class);
        Dictionary.IntDict intDict = (Dictionary.IntDict) dict;
        assertThat(intDict.size()).isEqualTo(3);
        assertThat(intDict.get(0)).isEqualTo(100);
        assertThat(intDict.get(1)).isEqualTo(200);
        assertThat(intDict.get(2)).isEqualTo(300);
        assertThat(intDict.kind()).isEqualTo(PrimitiveKind.INT32);
    }

    @Test
    void readBinaryDictionary() {
        // Three BYTE_ARRAY values: "foo", "bar", "baz"
        // Each entry: 4-byte LE length prefix followed by UTF-8 bytes
        byte[] foo = "foo".getBytes(StandardCharsets.UTF_8);
        byte[] bar = "bar".getBytes(StandardCharsets.UTF_8);
        byte[] baz = "baz".getBytes(StandardCharsets.UTF_8);
        int totalSize = 3 * 4 + foo.length + bar.length + baz.length;

        ByteBuffer page = ByteBuffer.allocate(totalSize).order(LITTLE_ENDIAN);
        page.putInt(foo.length);
        page.put(foo);
        page.putInt(bar.length);
        page.put(bar);
        page.putInt(baz.length);
        page.put(baz);
        page.flip();

        Dictionary<?> dict =
                DictionaryDecoder.read(MemorySegment.ofBuffer(page), PrimitiveKind.BYTE_ARRAY, 3, OptionalInt.empty());

        assertThat(dict).isInstanceOf(Dictionary.BinaryDict.class);
        Dictionary.BinaryDict binaryDict = (Dictionary.BinaryDict) dict;
        assertThat(binaryDict.size()).isEqualTo(3);
        assertThat(binaryDict.get(0).toArray(JAVA_BYTE)).isEqualTo(foo);
        assertThat(binaryDict.get(1).toArray(JAVA_BYTE)).isEqualTo(bar);
        assertThat(binaryDict.get(2).toArray(JAVA_BYTE)).isEqualTo(baz);
        assertThat(binaryDict.kind()).isEqualTo(PrimitiveKind.BYTE_ARRAY);
    }

    @Test
    void readFloatDictionary() {
        // Two FLOAT values: 1.5f, 2.5f in little-endian IEEE 754
        ByteBuffer page = ByteBuffer.allocate(8).order(LITTLE_ENDIAN);
        page.putFloat(1.5f);
        page.putFloat(2.5f);
        page.flip();

        Dictionary<?> dict =
                DictionaryDecoder.read(MemorySegment.ofBuffer(page), PrimitiveKind.FLOAT, 2, OptionalInt.empty());

        assertThat(dict).isInstanceOf(Dictionary.FloatDict.class);
        Dictionary.FloatDict floatDict = (Dictionary.FloatDict) dict;
        assertThat(floatDict.size()).isEqualTo(2);
        assertThat(floatDict.get(0)).isEqualTo(1.5f);
        assertThat(floatDict.get(1)).isEqualTo(2.5f);
        assertThat(floatDict.kind()).isEqualTo(PrimitiveKind.FLOAT);
    }

    /**
     * Verify that bulk decodeFloats on an RleDictionaryPageDecoder with a decoded dictionary matches successive next()
     * calls.
     */
    @Test
    void decodedDictionaryBulkDecodeFloats() {
        // Three FLOAT values: 1.5f, 2.5f, 3.5f in little-endian IEEE 754
        ByteBuffer dictPage = ByteBuffer.allocate(12).order(LITTLE_ENDIAN);
        dictPage.putFloat(1.5f);
        dictPage.putFloat(2.5f);
        dictPage.putFloat(3.5f);
        dictPage.flip();

        Dictionary<?> dict =
                DictionaryDecoder.read(MemorySegment.ofBuffer(dictPage), PrimitiveKind.FLOAT, 3, OptionalInt.empty());

        // Indexes [0, 1, 2, 0, 1, 2] encoded at bitWidth=2.
        // byte 0: 0x24, byte 1: 0x09 (same bit pattern as RleDictionaryPageDecoderTest)
        ByteBuffer indexPage = ByteBuffer.wrap(new byte[] {
            2, // bit width
            3, // RLE header: bit-packed, 1 group of 8
            (byte) 0x24, // byte 0 of packed indexes
            (byte) 0x09 // byte 1 of packed indexes
        });

        @SuppressWarnings("unchecked")
        PageDecoder<Float> decoder = new RleDictionaryPageDecoder<>((Dictionary<Float>) dict);
        decoder.load(MemorySegment.ofBuffer(indexPage), 6);

        float[] dst = new float[6];
        decoder.decodeFloats(6, dst, 0);

        assertThat(dst).containsExactly(1.5f, 2.5f, 3.5f, 1.5f, 2.5f, 3.5f);
    }
}
