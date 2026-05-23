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
package io.tileverse.parquetry.data.write.page;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.data.read.page.Dictionary;
import io.tileverse.parquetry.data.read.page.RleDictionaryPageDecoder;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.testsupport.ByteArrayWritableChannel;

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

    @ParameterizedTest(name = "{0}")
    @MethodSource("roundTripCases")
    void roundTripViaDecoder(String label, int[] indexes, int dictSize) throws Exception {
        ByteArrayWritableChannel out = new ByteArrayWritableChannel();
        new RleDictionaryEncoder().encode(indexes, indexes.length, out);

        // Build a synthetic dictionary that maps index i to i*10 so we can verify dereferencing.
        int[] dictValues = new int[dictSize];
        for (int i = 0; i < dictSize; i++) {
            dictValues[i] = i * 10;
        }
        Dictionary.IntDict dict = new Dictionary.IntDict(IntBuffer.wrap(dictValues));
        RleDictionaryPageDecoder<Integer> decoder = new RleDictionaryPageDecoder<>(dict);
        decoder.load(ByteBuffer.wrap(out.toByteArray()), indexes.length);

        for (int i = 0; i < indexes.length; i++) {
            assertThat(decoder.next()).as("%s [%d]", label, i).isEqualTo(indexes[i] * 10);
        }
    }
}
