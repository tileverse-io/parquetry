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
package io.tileverse.parquetry.internal.write.page;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.foreign.MemorySegment;
import java.nio.IntBuffer;
import java.util.stream.Stream;

import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.internal.read.page.Dictionary;
import io.tileverse.parquetry.internal.read.page.RleDictionaryPageDecoder;

class RleDictionaryEncoderTest {

    @Test
    void encodingMarkerIsRleDictionaryForV2AndPlainDictionaryForV1() {
        RleDictionaryEncoder encoder = new RleDictionaryEncoder();
        assertThat(encoder.parquetEncoding()).isEqualTo(Encoding.RLE_DICTIONARY);
        assertThat(encoder.parquetEncodingV1()).isEqualTo(Encoding.PLAIN_DICTIONARY);
    }

    static Stream<Arguments> roundTripCases() {
        return Stream.of(
                Arguments.of("empty", new int[0], 1),
                Arguments.of("singleIndex", new int[] {0}, 1),
                Arguments.of("twoDistinctIndexes", new int[] {0, 1, 0, 1}, 2),
                Arguments.of("threeDistinctIndexes", new int[] {0, 1, 2, 0, 1, 2}, 3),
                Arguments.of("longRunSameIndex", filled(64, 0), 1),
                Arguments.of("alternating", alternating(16), 2),
                Arguments.of("manyValuesWideRange", spreadIndexes(200, 32), 32),
                Arguments.of("indexesUpTo255", uniformlySpread(256), 256));
    }

    private static int[] filled(int n, int value) {
        int[] values = new int[n];
        for (int i = 0; i < n; i++) {
            values[i] = value;
        }
        return values;
    }

    private static int[] alternating(int n) {
        int[] values = new int[n];
        for (int i = 0; i < n; i++) {
            values[i] = i & 1;
        }
        return values;
    }

    private static int[] spreadIndexes(int n, int maxIndex) {
        int[] values = new int[n];
        for (int i = 0; i < n; i++) {
            values[i] = i % maxIndex;
        }
        return values;
    }

    private static int[] uniformlySpread(int dictSize) {
        int[] values = new int[dictSize];
        for (int i = 0; i < dictSize; i++) {
            values[i] = i;
        }
        return values;
    }

    @Test
    void bitWidthZeroStreamIsReadableByAStrictReader() throws Exception {
        // A column chunk whose dictionary holds a single distinct value indexes every page entry as 0, which yields a
        // zero bit width. parquetry's own decoder tolerates an empty index stream at bit width 0, but a strict reader
        // (parquet-java) reads the leading bit-width byte and then needs a run header declaring the value count; an
        // empty stream makes it throw "Reading past RLE/BitPacking stream". The encoder must emit the run header.
        int[] indexes = filled(40, 0);
        GrowableByteSink out = new GrowableByteSink(64);
        new RleDictionaryEncoder().encode(indexes, indexes.length, out);
        byte[] encoded = out.toByteArray();

        assertThat(encoded[0]).as("bit width byte").isZero();
        assertThat(encoded).as("run header present after the bit-width byte").hasSizeGreaterThan(1);

        int bitWidth = encoded[0] & 0xff;
        try (InputStream body = new ByteArrayInputStream(encoded, 1, encoded.length - 1)) {
            RunLengthBitPackingHybridDecoder decoder = new RunLengthBitPackingHybridDecoder(bitWidth, body);
            for (int i = 0; i < indexes.length; i++) {
                assertThat(decoder.readInt()).as("index %d", i).isZero();
            }
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("roundTripCases")
    void roundTripViaDecoder(String label, int[] indexes, int dictSize) throws Exception {
        GrowableByteSink out = new GrowableByteSink(64);
        new RleDictionaryEncoder().encode(indexes, indexes.length, out);

        // Build a synthetic dictionary that maps index i to i*10 so we can verify dereferencing.
        int[] dictValues = new int[dictSize];
        for (int i = 0; i < dictSize; i++) {
            dictValues[i] = i * 10;
        }
        Dictionary.IntDict dict = new Dictionary.IntDict(IntBuffer.wrap(dictValues));
        RleDictionaryPageDecoder<Integer> decoder = new RleDictionaryPageDecoder<>(dict);
        decoder.load(MemorySegment.ofArray(out.toByteArray()), indexes.length);

        for (int i = 0; i < indexes.length; i++) {
            assertThat(decoder.next()).as("%s [%d]", label, i).isEqualTo(indexes[i] * 10);
        }
    }
}
