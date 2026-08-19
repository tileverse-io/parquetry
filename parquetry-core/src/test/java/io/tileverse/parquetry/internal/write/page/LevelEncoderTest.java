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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;
import java.util.Random;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.internal.read.page.LevelDecoder;

class LevelEncoderTest {

    @Test
    void rejectsNegativeMaxLevel() {
        assertThatThrownBy(() -> new LevelEncoder(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-negative");
    }

    @Test
    void bitWidthZeroEmitsNoBytes() throws Exception {
        LevelEncoder encoder = new LevelEncoder(0);
        GrowableByteSink out = new GrowableByteSink(64);
        int written = encoder.encode(new int[] {0, 0, 0, 0}, 4, out);
        assertThat(written).isZero();
        assertThat(out.size()).isZero();

        LevelDecoder decoder = new LevelDecoder(0);
        decoder.load(MemorySegment.ofArray(out.toByteArray()));
        int[] decoded = new int[4];
        decoder.decode(4, decoded, 0);
        assertThat(decoded).containsExactly(0, 0, 0, 0);
    }

    static Stream<Arguments> roundTripCases() {
        return Stream.of(
                Arguments.of("singleRunLevel1", 1, new int[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1}),
                Arguments.of("alternating01", 1, new int[] {0, 1, 0, 1, 0, 1, 0, 1}),
                Arguments.of("threeLevelsRuns", 2, new int[] {0, 0, 0, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 0, 0}),
                Arguments.of("eightLevels", 7, sequentialMod(120, 8)),
                Arguments.of("longSingleRun", 3, filled(200, 3)),
                Arguments.of("randomSmall", 7, randomLevels(64, 8, 42L)),
                Arguments.of("randomLarge", 15, randomLevels(500, 16, 99L)),
                // bitWidth=3 lower edge (maxLevel=4) and bitWidth=7 upper edge (maxLevel=127): the bit-width boundary
                // calculations that drive RLE/bit-pack are easy to off-by-one if the range bounds are not tested.
                Arguments.of("maxLevel4", 4, randomLevels(120, 5, 7L)),
                Arguments.of("maxLevel127", 127, randomLevels(120, 128, 11L)));
    }

    private static int[] sequentialMod(int n, int modulus) {
        int[] values = new int[n];
        for (int i = 0; i < n; i++) {
            values[i] = i % modulus;
        }
        return values;
    }

    private static int[] filled(int n, int value) {
        int[] values = new int[n];
        for (int i = 0; i < n; i++) {
            values[i] = value;
        }
        return values;
    }

    private static int[] randomLevels(int n, int levelCardinality, long seed) {
        Random random = new Random(seed);
        int[] values = new int[n];
        for (int i = 0; i < n; i++) {
            values[i] = random.nextInt(levelCardinality);
        }
        return values;
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("roundTripCases")
    void roundTripViaLevelDecoder(String label, int maxLevel, int[] levels) throws Exception {
        GrowableByteSink out = new GrowableByteSink(64);
        new LevelEncoder(maxLevel).encode(levels, levels.length, out);

        LevelDecoder decoder = new LevelDecoder(LevelDecoder.computeBitWidth(maxLevel));
        decoder.load(MemorySegment.ofArray(out.toByteArray()));

        int[] decoded = new int[levels.length];
        decoder.decode(levels.length, decoded, 0);
        assertThat(decoded).as(label).containsExactly(levels);
    }

    @Test
    void emptyInputEmitsNoBytes() throws Exception {
        GrowableByteSink out = new GrowableByteSink(64);
        int written = new LevelEncoder(3).encode(new int[0], 0, out);
        assertThat(written).isZero();
    }
}
